package_index_mapping = {
    "mappings": {
        "properties": {
            "packageId": {"type": "keyword"},
            "availableMonths": {"type": "keyword"},
            "packageName": {"type": "text"},
            "packageTheme": {"type": "text"},
            "days": {"type": "integer"},
            "cities": {
                "type": "nested",
                "properties": {
                    "cityName": {"type": "text"},
                    "aliasCityName": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "geoLocation": {"type": "geo_point"},
                },
            },
            "highlights": {"type": "text"},
            "pdfName": {"type": "text"},
            "price": {"type": "integer"},
            "minimumPrice": {"type": "integer"},
            "packageData": {"type": "text"},
            "packageSummary": {"type": "text"},
            "thumbnailImage": {"type": "text"},
            "images": {"type": "text"},
            "visitingCountries": {"type": "text"},
            "departureCities": {
                "type": "nested",
                "properties": {
                    "cityName": {"type": "text"},
                    "cityCode": {"type": "keyword"},
                    "ltItineraryCode": {"type": "keyword"},
                    "holidayLtPricingId": {"type": "keyword"},
                },
            },
            "packageItinerary": {
                "type": "object",
                "properties": {
                    "summary": {"type": "text"},
                    "itinerary": {
                        "type": "nested",
                        "properties": {
                            "day": {"type": "integer"},
                            "description": {"type": "text"},
                            "mealDescription": {"type": "text"},
                            "overnightStay": {"type": "text"},
                        },
                    },
                },
            },
            "hotels": {"type": "text"},
            "meals": {"type": "text"},
            "visa": {"type": "text"},
            "transfer": {"type": "text"},
            "sightseeing": {"type": "text"},
            "inclusions": {"type": "text"},
            "exclusions": {"type": "text"},
            "termsAndConditions": {"type": "text"},
            "pkgSubtypeId": {"type": "integer"},
            "pkgSubtypeName": {"type": "text"},
            "pkgTypeId": {"type": "integer"},
            "isFlightIncluded": {"type": "keyword"},
            "holidayPlusSubType": {"type": "integer"},
            "productId": {"type": "integer"},
            "hashKey": {"type": "keyword"},
        }
    }
}


conversation_index_mapping = {
    "mappings": {
        "properties": {
            "conversationId": {"type": "keyword"},
            "userId": {"type": "keyword"},
            "booking_date": {
                "type": "date",
                "format": "strict_date_optional_time_nanos",
            },
            "chat_channel": {"type": "keyword"},
            "chat_model_name": {"type": "keyword"},
            "chat_model_version": {"type": "keyword"},
            "chat_modified": {
                "type": "date",
                "format": "strict_date_optional_time_nanos",
            },
            "chat_name": {"type": "keyword"},
            "chat_status": {"type": "keyword"},
            "chat_started": {
                "type": "date",
                "format": "strict_date_optional_time_nanos",
            },
            "chat_summary": {"type": "text"},
            "conversation": {
                "type": "nested",
                "properties": {
                    "chat_id": {"type": "keyword"},
                    "chat_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "content": {"type": "text"},
                    "message_id": {"type": "keyword"},
                    "modified_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "rating": {"type": "float"},
                    "role": {"type": "keyword"},
                    "sequence_id": {"type": "integer"},
                    "type": {"type": "keyword"},
                },
            },
            "customerId": {"type": "keyword"},
            "dataset_version": {"type": "keyword"},
            "opportunity_id": {"type": "keyword"},
            "packages_saved": {
                "type": "nested",
                "properties": {
                    "packageId": {"type": "keyword"},
                    "saved_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                },
            },
            "handoff_history": {
                "type": "nested",
                "properties": {
                    "agent_id": {"type": "keyword"},
                    "request_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "assign_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "accept_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "end_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos",
                    },
                    "wait_time_sec": {"type": "float"},
                    "duration_sec": {"type": "float"},
                    "end_reason": {"type": "keyword"},
                },
            },
        }
    }
}

# Visa FAQ Index Mapping
visa_faq_mapping = {
    "mappings": {
        "properties": {
            "country_id": {"type": "integer"},
            "visa_info": {"type": "text"},
            "visitingCountry": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
        }
    }
}
