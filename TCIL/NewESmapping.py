package_index_mapping = {
  "mappings": {
    "properties": {
      "packageId": { "type": "keyword" },
      "availableMonths": {"type": "keyword"},
      "packageName": { "type": "text" },
      "packageTheme": { "type": "text" },
      "days": { "type": "integer" },
      "cities": {
        "type": "nested",
        "properties": {
          "cityName": { "type": "text" },
          "aliasCityName": {
            "type": "text",
            "fields": {
              "keyword": { "type": "keyword" }  
            }
          },
          "geoLocation": { "type": "geo_point" }
        }
      },
      "highlights": { "type": "text" },
      "pdfName": { "type": "text" },
      "price": { "type": "integer" },
      "minimumPrice": { "type": "integer" },
      "packageData": { "type": "text" },
      "packageSummary": { "type": "text" },
      "thumbnailImage": { "type": "text" },
      "images": { "type": "text" },
      "visitingCountries": { "type": "text" },
      "departureCities": {
        "type": "nested",
        "properties": {
          "cityName": { "type": "text" },
          "cityCode": { "type": "keyword" },
          "ltItineraryCode": { "type": "keyword" },
          "holidayLtPricingId": { "type": "keyword" }
        }
      },
      "packageItinerary": {
        "type": "object",
        "properties": {
          "summary": { "type": "text" },
          "itinerary": {
            "type": "nested", 
            "properties": {
              "day": {
                "type": "integer"
              },
              "description": {
                "type": "text"
              },
              "mealDescription": {
                "type": "text"
              },
              "overnightStay": {
                "type": "text"
              }
            }
          }
        }
      },
      "hotels": { "type": "text" },
      "hotels_list": { "type": "object", "enabled": False },
      "continents": {
        "type": "nested",
        "properties": {
          "continentId": { "type": "integer" },
          "continentName": { "type": "keyword" },
          "countryCode": { "type": "keyword" },
          "countryName": { "type": "keyword" }
        }
      },
      "meals": { "type": "text" },
      "visa": { "type": "text" },
      "transfer": { "type": "text" },
      "sightseeing": { "type": "text" },
      "tourManagerDescription": { "type": "text" },
      "flightDescription": { "type": "text" },
      "inclusions": { "type": "text" },
      "exclusions": { "type": "text" },
      "termsAndConditions": { "type": "text" },
      "pkgSubtypeId": { "type": "integer" },
      "pkgSubtypeName": { "type": "text" },
      "pkgTypeId": { "type": "integer" },
      "isFlightIncluded": { "type": "keyword" },
      "holidayPlusSubType": { "type": "integer" },
      "productId": { "type": "integer" },
      "hashKey": { "type": "keyword" },
      "serviceSlots": {
        "type": "object",
        "properties": {
          "flight_included": {
            "type": "keyword"
          },
          "visa_included": {
            "type": "boolean"
          },
          "travel_insurance_included": {
            "type": "boolean"
          },
          "entrance_fees_included": {
            "type": "boolean"
          },
          "airport_transfer_included": {
            "type": "boolean"
          },
          "tour_manager_included": {
            "type": "boolean"
          },
          "tips_included": {
            "type": "boolean"
          },
          "breakfast_included": {
            "type": "boolean"
          },
          "all_meals_included": {
            "type": "boolean"
          },
          "wheelchair_accessible": {
            "type": "boolean"
          },
          "senior_citizen_friendly": {
            "type": "boolean"
          }
        }
      },
      "sightseeingTypes": {
        "type": "keyword"
      },
      "packageTourType": {
        "type": "keyword"
      }
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
                "format": "strict_date_optional_time_nanos"
            },
            "chat_channel": {"type": "keyword"},
            "chat_model_name": {"type": "keyword"},
            "chat_model_version": {"type": "keyword"},
            "chat_modified": {  
                "type": "date",
                "format": "strict_date_optional_time_nanos"
            },
            "chat_name": {"type": "keyword"},
            "chat_status":{"type": "keyword"},
            "chat_started": {
                "type": "date",
                "format": "strict_date_optional_time_nanos"
            },
            "chat_summary": {"type": "text"},
            "conversation": {
                "type": "nested",
                "properties": {
                    "chat_id": {"type": "keyword"},
                    "chat_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos"
                    },
                    "content": {"type": "text"},
                    "message_id": {"type": "keyword"},
                    "modified_time": {
                        "type": "date",
                        "format": "strict_date_optional_time_nanos"
                    },
                    "rating": {"type": "float"},
                    "role": {"type": "keyword"},
                    "sequence_id": {"type": "integer"},
                    "type": {"type": "keyword"}
                }
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
                        "format": "strict_date_optional_time_nanos"
                    }
                }
            }
        }
    }
}

# Visa FAQ Index Mapping
visa_faq_mapping = {
    "mappings" : {
      "properties" : {
        "country_id" : {
          "type" : "integer"
        },
        "visa_info" : {
          "type" : "text"
        },
        "visitingCountry" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
      }
    }
  }

CAMPAIGN_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "campaignId": {"type": "keyword"},
            "packageIds": {"type": "keyword"},          
            "createdAt":  {"type": "date"}              
        }
    }
}

destination_index_mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "analyzer": {
                "city_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "cityName": {
                "type": "text",
                "analyzer": "city_analyzer",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "aliasCityName": {
                "type": "text",
                "analyzer": "city_analyzer",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "stateName": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "countryName": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "location": {
                "type": "geo_point"
            },
            "packageCount": {
                "type": "integer"
            },
            "lastUpdated": {
                "type": "date"
            }
        }
    }
}