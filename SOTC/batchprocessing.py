from fastapi import FastAPI, HTTPException, BackgroundTasks, APIRouter
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
import logging
import asyncio
import subprocess
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List

# Constants and external functions
from constants import MAX_RETRIES, TIMEOUT, es, SOTC_PACKAGE_INDEX, SOTC_RAW_INDEX, ENABLE_SCHEDULED_JOB
from fetchpackages import fetch_packages_for_month, fetch_package_dynamically
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError
from NewESmapping import package_index_mapping

# ------------------------------------------------------------------------------
# FastAPI Setup
# ------------------------------------------------------------------------------
router = APIRouter()
app = FastAPI()
app.include_router(router)

# ------------------------------------------------------------------------------
# Global Scheduler Instance (Singleton)
# ------------------------------------------------------------------------------
scheduler = AsyncIOScheduler(timezone=timezone("Asia/Kolkata"))

# ------------------------------------------------------------------------------
# SMTP / Email Config
# ------------------------------------------------------------------------------
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "testerspora@gmail.com"
SMTP_PASS = "rgqc upfg wlbk gteg"
TO_EMAILS = ["jayanth@atirath.com", "bharat@atirath.com", "bhanu@atirath.com"]

def send_email_report(subject: str, body: str, recipient_list: List[str]):
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipient_list)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        logging.info(f"Email sent successfully to {recipient_list}.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


# -----------------------------------------------------------------------
#  5) Index Reset Logic
# -----------------------------------------------------------------------
def reset_index():
    """
    Deletes and recreates the index with the correct mapping.
    ⚠️ Use this carefully — it wipes all data!
    """
    if es.indices.exists(index=SOTC_PACKAGE_INDEX):
        logging.info(f"Deleting existing index: {SOTC_PACKAGE_INDEX}")
        es.indices.delete(index=SOTC_PACKAGE_INDEX)

    logging.info(f"Creating index: {SOTC_PACKAGE_INDEX} with correct mapping")
    es.indices.create(index=SOTC_PACKAGE_INDEX, body=package_index_mapping)

@router.post("/reset_index")
def reset_index_endpoint():
    try:
        reset_index()
        return {"message": "Index reset successfully"}
    except Exception as e:
        logging.error(f"Failed to reset index: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------
#  6) Main entry to trigger the entire batch process (endpoint)
# -----------------------------------------------------------------------
@router.post("/batch_process_all_packages")
async def batch_process_all_packages(background_tasks: BackgroundTasks):
    """
    1. Fetches available package IDs for all months and stores them in respective monthly indices.
    2. Filters all unique package IDs across months.
    3. Fetches package details in parallel and indexes them into `SOTC_PACKAGE_INDEX`.
    Uses background processing to prevent request timeouts.
    """
    try:
        months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]
        current_year = datetime.now().year

        # Schedule the processing in the background so the API call won't time out
        background_tasks.add_task(process_all_packages, months, current_year)

        return {"message": "Unified batch processing started for all packages."}
    except Exception as e:
        logging.error(f"Error starting batch processing: {str(e)}")
        raise HTTPException(status_code=500, detail="Error starting batch processing.")


# -----------------------------------------------------------------------
#  7) Core Logic to Clear & Process All Packages
# -----------------------------------------------------------------------
async def clear_index():
    """
    Delete all documents in both the main and raw indices.
    """
    try:
        for index in [SOTC_PACKAGE_INDEX, SOTC_RAW_INDEX]:
            exists = await asyncio.to_thread(es.indices.exists, index=index)
            if not exists:
                logging.info(f"Index {index} does not exist, skipping clear.")
                continue
            logging.info(f"Clearing all documents in {index}...")
            await asyncio.to_thread(
                es.delete_by_query,
                index=index,
                body={"query": {"match_all": {}}}
            )
            logging.info(f"All documents cleared from {index}.")
    except Exception as e:
        logging.error(f"Error clearing index: {str(e)}")
        raise

async def process_all_packages(months: List[str], current_year: int):
    """
    1. Generates a 12-month cycle starting from the current month.
    2. Fetches package IDs for all months in parallel.
    3. Tracks package availability months in a shared dictionary.
    4. Clears the index before bulk indexing.
    5. Fetches package details and indexes them into `SOTC_PACKAGE_INDEX`.
    Returns a dict with summary stats (failed months, total packages, summary generation counts).
    """
    logging.info("Starting 12-month cycle batch processing.")
    
    failed_months = []
    package_months = {}  # {package_id: [available_months]}
    lock = asyncio.Lock()  # For thread-safe updates

    # Generate 12-month cycle starting from the current month
    current_month_index = datetime.now().month - 1  # 0-based index

    # Create tasks for the 12-month cycle
    tasks = []
    for offset in range(12):
        month_idx = (current_month_index + offset) % 12
        year = current_year + ((current_month_index + offset) // 12)
        month_name = months[month_idx]
        tasks.append(
            fetch_and_store_monthly_packages(month_name, year, package_months, lock)
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any failed months
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Failed to process month {months[i]}: {result}")
            failed_months.append(months[i])

    if failed_months:
        logging.error(f"Failed months: {', '.join(failed_months)}")
    else:
        logging.info("All months processed successfully.")

    # Only clear the index if there's something to replace
    if package_months:
        await clear_index()
    else:
        logging.warning("No packages fetched. Skipping index clearing to retain existing data.")


    # Now process package details in parallel
    summary_stats = await process_unique_packages(package_months)

    # Return stats
    return {
        "failed_months": failed_months,
        "total_packages": len(package_months),
        "summary_stats": summary_stats
    }

async def fetch_and_store_monthly_packages(
    month: str,
    year: int,
    package_months: dict,
    lock: asyncio.Lock
):
    """
    Fetch package IDs for a given month and track their availability months.
    """
    try:
        month_index = ["january", "february", "march", "april", "may", "june",
                       "july", "august", "september", "october", "november", "december"].index(month)

        # Use 1-based month for the external API
        month_of_travel = f"{month_index:02d}-{year}"
        month_key = f"{month}_{year}"

        # STEP 1: Fetch package IDs with retries
        attempt = 0
        package_ids = None
        while attempt < MAX_RETRIES:
            try:
                logging.info(f"Fetching packages for {month_of_travel} (Attempt {attempt + 1}/{MAX_RETRIES})...")
                package_ids = await fetch_packages_with_timeout(month_of_travel)
                if package_ids:
                    logging.info(f"Fetched {len(package_ids)} packages for {month_key}")
                    break

            except ReadTimeout:
                logging.warning(f"Read timeout fetching {month_key}, retrying...")
            except ConnectionError:
                logging.warning(f"Connection error fetching {month_key}, retrying...")
            except HTTPError as http_err:
                logging.error(f"HTTP error fetching {month_key}: {http_err}")
                break  # Don't retry for HTTP errors
            except Exception as e:
                logging.error(f"Unexpected error fetching {month_key}: {str(e)}")
                break

            attempt += 1
            await asyncio.sleep(2 ** attempt)  # Exponential backoff

        if not package_ids:
            logging.error(f"Failed to fetch packages for {month_key} after {MAX_RETRIES} attempts")
            return

        # STEP 2: Track package-month relationships
        async with lock:
            for package_id in package_ids:
                if package_id not in package_months:
                    package_months[package_id] = []
                if month_key not in package_months[package_id]:
                    package_months[package_id].append(month_key)

        logging.info(f"Tracked {len(package_ids)} packages for {month_key}")

    except Exception as e:
        logging.error(f"Error processing month {month}: {str(e)}")


async def fetch_packages_with_timeout(month_of_travel):
    """
    Wrapper to enforce a timeout when calling `fetch_packages_for_month()`.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(fetch_packages_for_month, month_of_travel),
            timeout=TIMEOUT
        )
    except asyncio.TimeoutError:
        logging.error(f"Timeout error: Fetching packages for {month_of_travel} took longer than {TIMEOUT} seconds.")
        return []

async def process_unique_packages(package_months: dict):
    """
    Fetches package details concurrently for unique package IDs and stores them in the main index (`SOTC_PACKAGE_INDEX`).
    Includes retry logic for failed documents during bulk indexing.
    Tracks and logs the status of `packageSummary` generation in real-time.
    Returns a dict with summary generation stats.
    """
    logging.info(f"🚀 Processing {len(package_months)} unique packages for indexing.")
    summary_stats = {"generated": 0, "failed": 0}

    if not package_months:
        logging.warning("⚠️ No unique packages found after filtering. Skipping detailed processing.")
        return summary_stats

    SEMAPHORE_LIMIT = 50
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async def limited_fetch_and_store(package_id):
        async with semaphore:
            result = await fetch_and_store_package(package_id, package_months.get(package_id, []))
            if isinstance(result, tuple):
                summary = result[0].get("doc", {}).get("packageSummary")
                if summary and not summary.strip().lower().startswith("error"):
                    summary_stats["generated"] += 1
                    logging.info(f"Summary generated for package {package_id}: Yes")
                else:
                    summary_stats["failed"] += 1
                    logging.info(f"Summary generated for package {package_id}: No")
            else:
                summary_stats["failed"] += 1
            return result

    batch_size = 500
    package_list = list(package_months.keys())
    results = []

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(package_list), desc="Processing packages", unit="pkg")
    except ImportError:
        pbar = None

    for i in range(0, len(package_list), batch_size):
        batch = package_list[i: i + batch_size]
        logging.info(f"🔄 Processing batch {i // batch_size + 1}: {len(batch)} packages.")

        tasks = [limited_fetch_and_store(pkg) for pkg in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        results.extend(batch_results)

        if pbar:
            pbar.update(len(batch))

        await asyncio.sleep(1)

    if pbar:
        pbar.close()

    es_actions = [res[0] for res in results if isinstance(res, tuple)]
    es_raw_actions = [res[1] for res in results if isinstance(res, tuple)]
    failed_packages = [res for res in results if isinstance(res, Exception)]

    if es_actions:
        try:
            retry_count = 0
            while retry_count < MAX_RETRIES:
                success, failed = await asyncio.to_thread(
                    helpers.bulk, es, es_actions, chunk_size=500, stats_only=True
                )
                if failed == 0:
                    break
                logging.warning(f"Retry {retry_count + 1}/{MAX_RETRIES}: {failed} documents failed.")
                retry_count += 1
                await asyncio.sleep(2 ** retry_count)

            logging.info(f"📌 Successfully indexed {success} packages into {SOTC_PACKAGE_INDEX}. Failed: {failed}")

            # 🔄 Run aliasCityName update script
            try:
                script_path = "/home/gcp-admin/thomascook-travelplanner/Elastic Search/DataExtractionTools/update_aliasName.py"
                logging.info("Running post-indexing aliasCityName update script...")
                result = subprocess.run(["python3", script_path], capture_output=True, text=True)

                if result.returncode == 0:
                    logging.info("✅ aliasCityName update script ran successfully.")
                    logging.info(result.stdout)
                else:
                    logging.error("❌ aliasCityName update script failed.")
                    logging.error(result.stderr)

            except Exception as e:
                logging.error(f"Error running aliasCityName update script: {e}")

        except Exception as e:
            logging.error(f"❌ Error during bulk indexing: {str(e)}")

    if es_raw_actions:
        try:
            await asyncio.to_thread(
                helpers.bulk, es, es_raw_actions, chunk_size=500, stats_only=True
            )
            logging.info(f"📌 Successfully indexed {len(es_raw_actions)} raw packages into {SOTC_RAW_INDEX}.")
        except Exception as e:
            logging.error(f"❌ Error during raw bulk indexing: {str(e)}")

    logging.info(f"✅ Batch processing completed. Processed: {len(es_actions)}, Failed: {len(failed_packages)}")
    logging.info(f"📊 Summary generation stats: Generated: {summary_stats['generated']}, Failed: {summary_stats['failed']}")

    return summary_stats



async def ensure_raw_index_exists():
    """
    Checks if the raw Elasticsearch index exists; if not, creates it with minimal mapping.
    """
    try:
        exists = await asyncio.to_thread(es.indices.exists, index=SOTC_RAW_INDEX)
        if not exists:
            logging.info(f"Index {SOTC_RAW_INDEX} does not exist. Creating...")
            raw_index_mapping = {
                "mappings": {
                    "properties": {
                        "packageId": {"type": "keyword"},
                        "fetchedAt": {"type": "date"},
                        "raw_response": {"type": "object", "enabled": False}
                    }
                }
            }
            await asyncio.to_thread(es.indices.create, index=SOTC_RAW_INDEX, body=raw_index_mapping)
            logging.info(f"Index {SOTC_RAW_INDEX} created.")
        else:
            logging.info(f"Index {SOTC_RAW_INDEX} already exists.")
    except Exception as e:
        logging.error(f"Error ensuring raw index exists: {str(e)}")
        raise


async def fetch_and_store_package(package_id: str, available_months: list):
    """
    Fetches package details for a single package and prepares it for bulk indexing.
    Includes `availableMonths` in the document.
    """
    try:
        await ensure_index_exists()
        await ensure_raw_index_exists()

        logging.info(f"Fetching details for package ID: {package_id}")
        result = await asyncio.to_thread(fetch_package_dynamically, package_id)

        if not result or not result.get("processed"):
            logging.warning(f"Skipping package {package_id} due to missing data.")
            return None

        package_data = result["processed"]
        raw_pdp_data = result["raw"]
        
        summary_status = "generated" if package_data.get("packageSummary") else "not generated"
        logging.info(f"Processed package {package_id} | Summary: {summary_status}")


        itinerary_data = package_data.get("packageItinerary", {"summary": "", "itinerary": []})
        formatted_itinerary = [
            {
                "day": item.get("day"),
                "description": item.get("description", ""),
                "mealDescription": item.get("mealDescription", ""),
                "overnightStay": item.get("overnightStay", "")
            }
            for item in itinerary_data.get("itinerary", [])
        ]

        departure_cities = [
            {
                "cityName": city.get("cityName", ""),
                "cityCode": city.get("cityCode", ""),
                "ltItineraryCode": city.get("ltItineraryCode", "")
            }
            for city in package_data.get("departureCities", [])
        ]

        processed_action = {
            "_op_type": "update", 
            "_index": SOTC_PACKAGE_INDEX,
            "_id": package_data["packageId"],
            "doc": {
                "packageId": package_data["packageId"],
                "availableMonths": available_months,
                "packageName": package_data["packageName"],
                "packageTheme": package_data.get("packageTheme", []),
                "days": package_data["days"],
                "cities": package_data.get("cities", []),
                "highlights": package_data.get("highlights", []),
                "pdfName": package_data.get("pdfName"),
                "price": package_data.get("minimumPrice"),
                "minimumPrice": package_data.get("minimumPrice"),
                "packageData": package_data["packageData"],
                "packageSummary": package_data.get("packageSummary"),
                "thumbnailImage": package_data["thumbnailImage"],
                "images": package_data.get("images", []),
                "visitingCountries": package_data.get("visitingCountries", []),
                "departureCities": departure_cities,
                "packageItinerary": {
                    "summary": itinerary_data.get("summary", ""),
                    "itinerary": formatted_itinerary
                },
                "hotels": package_data.get("hotels"),
                "hotels_list": package_data.get("hotels_list"),
                "meals": package_data.get("meals"),
                "visa": package_data.get("visa"),
                "sightseeing": package_data.get("sightseeing"),
                "inclusions": package_data.get("inclusions"),
                "exclusions": package_data.get("exclusions"),
                "termsAndConditions": package_data.get("termsAndConditions"),
                "hashKey": package_data.get("hashKey"),
                "pkgSubtypeId": package_data["pkgSubtypeId"],
                "pkgSubtypeName": package_data["pkgSubtypeName"],
                "pkgTypeId": package_data["pkgTypeId"],
                "isFlightIncluded": package_data.get("isFlightIncluded"),
                "holidayPlusSubType": package_data.get("holidayPlusSubType"),
                "productId": package_data.get("productId")
            },
            "doc_as_upsert": True
        }

        raw_action = {
            "_op_type": "index",
            "_index": SOTC_RAW_INDEX,
            "_id": package_data["packageId"],
            "packageId": package_data["packageId"],
            "fetchedAt": datetime.utcnow().isoformat(),
            "raw_response": raw_pdp_data
        }

        return processed_action, raw_action
    except Exception as e:
        logging.error(f"Error fetching package {package_id}: {str(e)}")
        return e

async def ensure_index_exists():
    """
    Checks if the Elasticsearch index exists; if not, creates it with the correct mapping.
    """
    try:
        exists = await asyncio.to_thread(es.indices.exists, index=SOTC_PACKAGE_INDEX)
        if not exists:
            logging.info(f"Index {SOTC_PACKAGE_INDEX} does not exist. Creating...")
            await asyncio.to_thread(es.indices.create, index=SOTC_PACKAGE_INDEX, body=package_index_mapping)
            logging.info(f"Index {SOTC_PACKAGE_INDEX} created.")
        else:
            logging.info(f"Index {SOTC_PACKAGE_INDEX} already exists.")
    except Exception as e:
        logging.error(f"Error ensuring index exists: {str(e)}")
        raise


# -----------------------------------------------------------------------
#  8) The APScheduler job to run at 01:50 IST daily & email stats
# -----------------------------------------------------------------------
async def run_scheduled_batch_process():
    logging.info("Starting scheduled batch process...")
    months = [ "january", "february", "march", "april", "may", "june",
               "july", "august", "september", "october", "november", "december" ]
    this_year = datetime.now().year
    try:
        result = await process_all_packages(months, this_year)
        body = (
            f"Hello,\n\n"
            f"The daily batch process has completed.\n\n"
            f"Failed Months: {', '.join(result['failed_months']) or 'None'}\n"
            f"Total Packages: {result['total_packages']}\n"
            f"Summary Generated: {result['summary_stats']['generated']}\n"
            f"Summary Failed: {result['summary_stats']['failed']}\n\n"
            f"Regards,\nBatch Process Scheduler"
        )
        send_email_report("SOTC Daily Batch Processing Report", body, TO_EMAILS)
    except Exception as e:
        send_email_report("Daily Batch Processing FAILED", str(e), TO_EMAILS)


async def schedule_daily_job():
    if not ENABLE_SCHEDULED_JOB:
        logging.info("Scheduled job is disabled via ENABLE_SCHEDULED_JOB flag.")
        return
    if scheduler.running:
        logging.info("Scheduler already running. Skipping new start.")
        return

    scheduler.add_job(
        run_scheduled_batch_process,
        trigger="cron",
        hour=1,
        minute=50,
        id="run_scheduled_batch_process"
    )
    scheduler.start()
    logging.info("Scheduler started: batch will run daily at 01:50 IST.")



# -----------------------------------------------------------------------
#  9) (Optional) A quick test endpoint to trigger manually
# -----------------------------------------------------------------------
@app.get("/trigger_now")
async def trigger_now(background_tasks: BackgroundTasks):
    """
    Manually trigger the batch process + email to verify everything works.
    """
    background_tasks.add_task(run_scheduled_batch_process)
    return {"detail": "Manual batch process triggered. Check logs and email."}
