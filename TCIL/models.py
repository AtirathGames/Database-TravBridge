from pydantic import BaseModel, Field
import logging
import json, requests
from typing import Dict, List, Optional, Union,Any
import os
from datetime import datetime

# Pydantic models

class GeoLocation(BaseModel):
    lat: float
    lon: float

class DayItinerary(BaseModel):
    day: int
    description: str
    mealDescription: str
    overnightStay: str

class PackageItinerary(BaseModel):
    summary: Optional[str] = ""
    itinerary: List[DayItinerary]

class DepartureCity(BaseModel):
    cityName: str
    cityCode: str
    ltItineraryCode: str
    holidayLtPricingId: str

class ItineraryData(BaseModel):
    packageId: str
    availableMonths:List[str]
    packageName: str
    days: Optional[int]
    cities: List[Dict[str, Union[str, Dict[str, float]]]]
    highlights: List[str]
    pdfName: Optional[str]
    price: Optional[int]
    packageData: str
    packageSummary: Optional[str] = None
    images: List[str]
    minimumPrice: Optional[int]
    thumbnailImage: Optional[str]
    packageTheme: List[str]
    visitingCountries: List[str]
    departureCities: List[str]  
    packageItinerary: PackageItinerary
    hotels: Optional[Union[str, List[str]]] = None
    hotels_list: Optional[Union[str, List[str], Dict[str, str]]] = None
    continents: Optional[List[Dict[str, Union[int, str]]]] = None
    packageTourType: Optional[List[str]] = None
    meals: Optional[Union[str, List[str]]] = None
    visa: Optional[Union[str, List[str]]] = None
    transfer: Optional[Union[str, List[str]]] = None
    sightseeing : Optional[Union[str, List[str]]] = None
    tourManagerDescription: Optional[str] = None
    flightDescription: Optional[str] = None
    inclusions: Optional[str] = None
    exclusions: Optional[str] = None
    termsAndConditions: Optional[str] = None
    pkgSubtypeId:Optional[int]
    pkgSubtypeName:str
    pkgTypeId:Optional[int]
    isFlightIncluded: Optional[str] = None
    holidayPlusSubType: Optional[int] = None
    productId: Optional[int] = None
    

class savedItineraryData(BaseModel):
    packageId: str
    availableMonths: Optional[Union[str, List[str]]] = None
    packageName: str
    days: Optional[int]
    cities: List[Dict[str, Union[str, Dict[str, float]]]]
    highlights: List[str]
    pdfName: Optional[str]
    price: Optional[int]
    packageSummary: Optional[str] = None
    images: List[str]
    minimumPrice: Optional[int]
    thumbnailImage: Optional[str]
    packageTheme: List[str]
    packageData: str
    departureCities: List[DepartureCity]  
    packageItinerary: PackageItinerary
    hotels: Optional[Union[str, List[str]]] = None
    hotels_list: Optional[Union[str, List[str], Dict[str, str]]] = None
    continents: Optional[List[Dict[str, Union[str, int]]]] = None
    packageTourType: Optional[List[str]] = None
    meals: Optional[Union[str, List[str]]] = None
    visa: Optional[Union[str, List[str]]] = None
    transfer: Optional[Union[str, List[str]]] = None
    sightseeing: Optional[Union[str, List[str]]] = None
    tourManagerDescription: Optional[str] = None
    flightDescription: Optional[str] = None
    inclusions: Optional[str] = None
    exclusions: Optional[str] = None
    termsAndConditions: Optional[str] = None
    pkgSubtypeId: Optional[int]
    pkgSubtypeName: str
    pkgTypeId: Optional[int]
    isFlightIncluded: Optional[str] = None
    holidayPlusSubType: Optional[int] = None
    productId: Optional[int] = None
    flightsAvailability: Optional[str] = None
    serviceSlots: Optional[Dict[str, Union[bool, str, None]]] = None  # NEW: Service slots
    constructed_thumbnailImage: Optional[str] = None
    constructed_images: Optional[List[str]] = None
    

class ItemOut(BaseModel):
    id: str
    itinerary_data: savedItineraryData
    score: float
    departureCity_details: Optional[Dict[str, str]] = None
    fareCalendar: Optional[Dict[str, Any]] = None

class SearchItem(BaseModel):
    text: str
    index: str
    geoLocation: Optional[GeoLocation] = None
    search_type: Optional[str] = "match"
    search_results_size: Optional[int] = 10

class QueryRequest(BaseModel):
    text: str
    index: str
    days: Optional[int]
    budget: Optional[int]

class PackageSearchRequest(BaseModel):
    search_term: str
    departureCity: Optional[str] = "" 
    days: Optional[int] = 0
    budget: Optional[int] = 0
    number_of_people: Optional[int] = 0
    monthOfTravel: Optional[str] = "" 
    theme: Optional[str] = ""
    fareCalendar: Optional[bool] = False
    pkgSubtypeName: Optional[str] = ""  # Filter by GIT or FIT 

class AutoBudgetRequest(BaseModel):
    search_term: str

class ChatMessage(BaseModel):
    chat_id: str
    chat_time: datetime
    content: str
    message_id: str
    modified_time: datetime
    rating: Optional[float] = None
    role: str
    sequence_id: Optional[int] = None
    type: str

class SavedPackages(BaseModel):
    packageId: str
    saved_time: datetime

class Conversation(BaseModel):
    conversationId: str
    userId: str

    booking_date: Optional[datetime] = None
    chat_channel: Optional[str] = None
    chat_model_name: Optional[str] = None
    chat_model_version: Optional[str] = None
    chat_modified: datetime
    chat_name: str
    chat_started: datetime
    chat_summary: Optional[str] = None

    conversation: List[ChatMessage]

    customerId: Optional[str] = None
    dataset_version: Optional[str] = None
    opportunity_id: Optional[str] = None

    packages_saved: Optional[List[SavedPackages]] = None

class UserIdRequest(BaseModel):
    userId: str

class ConversationIdRequest(BaseModel):
    conversationId: str

class UpdateChatNameRequest(BaseModel):
    conversationId: str
    new_chat_name: str

class DeleteConversationRequest(BaseModel):
    conversationId: str

TOKEN_CACHE = {
    "requestId": None,
    "sessionId": None,
    "expires_at": 0  # epoch time
}

GEOCODE_CACHE = {}

class FilterRequest(BaseModel):
    chat_started_from: Optional[str] = None
    chat_started_to: Optional[str] = None
    booking_date_from: Optional[str] = None
    booking_date_to: Optional[str] = None
    chat_channel: Optional[str] = None
    opportunity_id: Optional[str] = None   # Checkbox: if provided then filter docs with an opportunity_id field
    userId: Optional[str] = None            # Dropdown: "all", "registered", "guest"
    only_tool_conversations: Optional[bool] = None  # Checkbox: filter docs where all messages have role 'tool'
    page: Optional[int] = None
    count: Optional[int] = None

class ExportFilterRequest(BaseModel):
    chat_started_from: str | None = None
    chat_started_to: str | None = None
    chat_channel: str | None = None
    opportunity_id: bool | None = False
    userId: str | None = "all"
    only_tool_conversations: bool | None = False


class DateRangeRequest(BaseModel):
    from_date: str  # YYYY-MM-DD
    to_date: str    # YYYY-MM-DD

from pydantic import BaseModel, Field
from typing import List

class CampaignCreateRequest(BaseModel):
    campaignId: str  = Field(..., example="c00001")
    packageIds: List[str] = Field(..., example=["12345","2345","5678","5679"])
