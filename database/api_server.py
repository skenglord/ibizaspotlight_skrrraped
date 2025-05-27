"""
FastAPI server for accessing MongoDB event data with quality filtering
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
import uvicorn

# Initialize FastAPI app
app = FastAPI(
    title="Tickets Ibiza Event API",
    description="API for accessing quality-filtered event data",
    version="1.0.0"
)

# Add CORS middleware for your app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for your app's domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client.tickets_ibiza_events


# Pydantic models for response
class QualityScore(BaseModel):
    overall: float = Field(..., ge=0, le=1)
    title: float = Field(..., ge=0, le=1)
    location: float = Field(..., ge=0, le=1)
    dateTime: float = Field(..., ge=0, le=1)
    lineUp: float = Field(..., ge=0, le=1)
    ticketInfo: float = Field(..., ge=0, le=1)


class Location(BaseModel):
    venue: Optional[str]
    address: Optional[str]
    city: Optional[str]
    country: Optional[str]
    coordinates: Optional[Dict[str, float]]


class Artist(BaseModel):
    name: str
    headliner: Optional[bool]
    genre: Optional[str]


class TicketInfo(BaseModel):
    status: Optional[str]
    startingPrice: Optional[float]
    currency: Optional[str]
    url: Optional[str]
    provider: Optional[str]


class Event(BaseModel):
    id: str = Field(..., alias="_id")
    url: str
    title: str
    location: Optional[Location]
    dateTime: Optional[Dict[str, Any]]
    lineUp: Optional[List[Artist]]
    ticketInfo: Optional[TicketInfo]
    fullDescription: Optional[str]
    images: Optional[List[str]]
    qualityScore: Optional[QualityScore]
    
    class Config:
        populate_by_name = True


class EventSummary(BaseModel):
    id: str = Field(..., alias="_id")
    url: str
    title: str
    venue: Optional[str]
    date: Optional[str]
    qualityScore: float
    status: Optional[str]
    
    class Config:
        populate_by_name = True


class QualityStats(BaseModel):
    totalEvents: int
    averageQuality: float
    distribution: Dict[str, int]
    topVenues: List[Dict[str, Any]]


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Tickets Ibiza Event API",
        "database": "connected" if client else "disconnected"
    }


@app.get("/api/events", response_model=List[EventSummary], tags=["Events"])
async def get_events(
    min_quality: float = Query(0.7, ge=0, le=1, description="Minimum quality score"),
    venue: Optional[str] = Query(None, description="Filter by venue name"),
    future_only: bool = Query(True, description="Only show future events"),
    limit: int = Query(50, ge=1, le=200, description="Number of results"),
    skip: int = Query(0, ge=0, description="Skip results for pagination")
):
    """
    Get events filtered by quality and other criteria
    """
    # Build query
    query = {"_quality.overall": {"$gte": min_quality}}
    
    if venue:
        query["location.venue"] = {"$regex": venue, "$options": "i"}
    
    if future_only:
        query["dateTime.start"] = {"$gte": datetime.utcnow()}
    
    # Execute query
    events = list(db.events.find(
        query,
        {
            "_id": {"$toString": "$_id"},
            "url": 1,
            "title": 1,
            "venue": "$location.venue",
            "date": "$dateTime.displayText",
            "qualityScore": "$_quality.overall",
            "status": "$ticketInfo.status"
        }
    ).sort("dateTime.start", 1).skip(skip).limit(limit))
    
    return events


@app.get("/api/events/{event_id}", response_model=Event, tags=["Events"])
async def get_event(event_id: str):
    """
    Get detailed information for a specific event
    """
    from bson import ObjectId
    
    try:
        event = db.events.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Transform for response
        event["_id"] = str(event["_id"])
        event["qualityScore"] = event.get("_quality", {}).get("scores", {})
        
        return event
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/events/search/{search_term}", response_model=List[EventSummary], tags=["Events"])
async def search_events(
    search_term: str,
    min_quality: float = Query(0.6, ge=0, le=1),
    limit: int = Query(20, ge=1, le=50)
):
    """
    Search events by text with quality filtering
    """
    events = list(db.events.find(
        {
            "$text": {"$search": search_term},
            "_quality.overall": {"$gte": min_quality}
        },
        {
            "_id": {"$toString": "$_id"},
            "url": 1,
            "title": 1,
            "venue": "$location.venue",
            "date": "$dateTime.displayText",
            "qualityScore": "$_quality.overall",
            "status": "$ticketInfo.status",
            "score": {"$meta": "textScore"}
        }
    ).sort([("score", {"$meta": "textScore"})]).limit(limit))
    
    return events


@app.get("/api/venues", tags=["Venues"])
async def get_venues():
    """
    Get list of all venues with event counts
    """
    pipeline = [
        {"$group": {
            "_id": "$location.venue",
            "eventCount": {"$sum": 1},
            "avgQuality": {"$avg": "$_quality.overall"},
            "upcomingEvents": {
                "$sum": {
                    "$cond": [
                        {"$gte": ["$dateTime.start", datetime.utcnow()]},
                        1, 0
                    ]
                }
            }
        }},
        {"$match": {"_id": {"$ne": None}}},
        {"$sort": {"eventCount": -1}},
        {"$project": {
            "venue": "$_id",
            "eventCount": 1,
            "avgQuality": {"$round": ["$avgQuality", 3]},
            "upcomingEvents": 1,
            "_id": 0
        }}
    ]
    
    venues = list(db.events.aggregate(pipeline))
    return venues


@app.get("/api/venues/{venue_name}/events", response_model=List[EventSummary], tags=["Venues"])
async def get_venue_events(
    venue_name: str,
    future_only: bool = Query(True),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get all events for a specific venue
    """
    query = {"location.venue": {"$regex": venue_name, "$options": "i"}}
    
    if future_only:
        query["dateTime.start"] = {"$gte": datetime.utcnow()}
    
    events = list(db.events.find(
        query,
        {
            "_id": {"$toString": "$_id"},
            "url": 1,
            "title": 1,
            "venue": "$location.venue",
            "date": "$dateTime.displayText",
            "qualityScore": "$_quality.overall",
            "status": "$ticketInfo.status"
        }
    ).sort("dateTime.start", 1).limit(limit))
    
    return events


@app.get("/api/stats/quality", response_model=QualityStats, tags=["Statistics"])
async def get_quality_stats():
    """
    Get overall quality statistics
    """
    # Overall stats
    pipeline = [
        {
            "$group": {
                "_id": None,
                "totalEvents": {"$sum": 1},
                "avgQuality": {"$avg": "$_quality.overall"},
                "excellent": {"$sum": {"$cond": [{"$gte": ["$_quality.overall", 0.9]}, 1, 0]}},
                "good": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$_quality.overall", 0.8]},
                        {"$lt": ["$_quality.overall", 0.9]}
                    ]}, 1, 0
                ]}},
                "fair": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$_quality.overall", 0.7]},
                        {"$lt": ["$_quality.overall", 0.8]}
                    ]}, 1, 0
                ]}},
                "poor": {"$sum": {"$cond": [{"$lt": ["$_quality.overall", 0.7]}, 1, 0]}}
            }
        }
    ]
    
    stats_result = list(db.events.aggregate(pipeline))
    
    if not stats_result:
        raise HTTPException(status_code=404, detail="No statistics available")
    
    stats = stats_result[0]
    
    # Top venues by quality
    venue_pipeline = [
        {"$group": {
            "_id": "$location.venue",
            "avgQuality": {"$avg": "$_quality.overall"},
            "eventCount": {"$sum": 1}
        }},
        {"$match": {"_id": {"$ne": None}}},
        {"$sort": {"avgQuality": -1}},
        {"$limit": 10},
        {"$project": {
            "venue": "$_id",
            "avgQuality": {"$round": ["$avgQuality", 3]},
            "eventCount": 1,
            "_id": 0
        }}
    ]
    
    top_venues = list(db.events.aggregate(venue_pipeline))
    
    return {
        "totalEvents": stats["totalEvents"],
        "averageQuality": round(stats["avgQuality"], 3),
        "distribution": {
            "excellent": stats["excellent"],
            "good": stats["good"],
            "fair": stats["fair"],
            "poor": stats["poor"]
        },
        "topVenues": top_venues
    }


@app.get("/api/upcoming", response_model=List[EventSummary], tags=["Events"])
async def get_upcoming_events(
    days: int = Query(7, ge=1, le=30, description="Number of days ahead"),
    min_quality: float = Query(0.75, ge=0, le=1),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Get upcoming events within specified days
    """
    end_date = datetime.utcnow() + timedelta(days=days)
    
    events = list(db.events.find(
        {
            "dateTime.start": {
                "$gte": datetime.utcnow(),
                "$lte": end_date
            },
            "_quality.overall": {"$gte": min_quality}
        },
        {
            "_id": {"$toString": "$_id"},
            "url": 1,
            "title": 1,
            "venue": "$location.venue",
            "date": "$dateTime.displayText",
            "qualityScore": "$_quality.overall",
            "status": "$ticketInfo.status"
        }
    ).sort("dateTime.start", 1).limit(limit))
    
    return events


@app.post("/api/events/{event_id}/refresh", tags=["Events"])
async def refresh_event(event_id: str):
    """
    Mark an event for re-scraping
    """
    from bson import ObjectId
    
    try:
        result = db.events.update_one(
            {"_id": ObjectId(event_id)},
            {
                "$set": {
                    "needsRefresh": True,
                    "refreshRequestedAt": datetime.utcnow()
                }
            }
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        
        return {"message": "Event marked for refresh", "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    # Run the server
    print("Starting Tickets Ibiza Event API...")
    print("API Documentation: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)