"""
quote_exporter.py — Export Dynamics LVP quotes to blob storage for PO crosswalk.

Pulls:
- lvp_quotes: Quote headers with customer, status, pricing
- lvp_quotelines: Line items with products, quantities, prices
- Links via lvp_quoteid -> _lvp_quote_value

Output: CSV files to Azure Blob for crosswalk lookup:
  - quotes_dynamics.csv: Quote# → Customer, status, total amount
  - quote_lines_dynamics.csv: Quote# → Line items with P21 mapping candidates

Schedule: Daily via Azure Function or local cron.
"""

import os
import sys
import json
import csv
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Dict, Optional

import requests

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from services.processing.blob_uploader import get_uploader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DYNAMICS_BASE = "https://enproinc.crm.dynamics.com/api/data/v9.2"
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "../../../scripts/dynamics_token.json")


def load_token() -> str:
    """Load Dynamics access token."""
    try:
        with open(TOKEN_FILE) as f:
            return json.load(f)["access_token"]
    except Exception as e:
        logger.error(f"Failed to load token from {TOKEN_FILE}: {e}")
        raise


def headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": "odata.maxpagesize=500",
    }


def fetch_all(token: str, entity: str, select: str, filter_query: Optional[str] = None, max_pages: int = 100) -> List[dict]:
    """Fetch all records from Dynamics with pagination."""
    all_records = []
    url = f"{DYNAMICS_BASE}/{entity}"
    params = {"$select": select}
    if filter_query:
        params["$filter"] = filter_query
    
    page = 0
    while page < max_pages:
        resp = requests.get(url, params=params, headers=headers(token), timeout=60)
        if resp.status_code != 200:
            logger.error(f"Error fetching {entity} page {page}: {resp.status_code}")
            break
        
        data = resp.json()
        records = data.get("value", [])
        all_records.extend(records)
        
        logger.info(f"{entity} page {page + 1}: {len(records)} records (total: {len(all_records)})")
        
        next_link = data.get("@odata.nextLink")
        if not next_link or not records:
            break
        
        url = next_link
        params = None
        page += 1
    
    return all_records


def get_recent_quotes(token: str, days_back: int = 365) -> List[dict]:
    """Get quotes modified in the last N days."""
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    filter_query = f"modifiedon ge {since}T00:00:00Z"
    
    select = (
        "lvp_quoteid,lvp_quotenumber,lvp_name,lvp_quotestatus,lvp_quotestatusname,"
        "lvp_extendedprice,lvp_probability,lvp_fob,lvp_freightterms,lvp_paymentterms,"
        "lvp_estcompletiondate,lvp_finalizeddate,lvp_orderreceiveddate,"
        "lvp_quotewithrevision,lvp_application,lvp_productlinemanufacturers,"
        "_lvp_customer_value,_lvp_contact_value,_lvp_salesrep_value,"
        "statecode,statuscode,createdon,modifiedon"
    )
    
    return fetch_all(token, "lvp_quotes", select, filter_query)


def get_quote_lines(token: str, days_back: int = 365) -> List[dict]:
    """Get quote lines for recent quotes."""
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    filter_query = f"modifiedon ge {since}T00:00:00Z"
    
    select = (
        "lvp_quotelineid,lvp_name,lvp_item,lvp_description,lvp_extendeddescription,"
        "lvp_quantity,lvp_enprocost,lvp_customernetpricepart,lvp_extendedprice,"
        "lvp_discount,lvp_markup,lvp_grossmargin,lvp_listprice,lvp_quotenumber,"
        "lvp_productgroup,_lvp_quote_value,_lvp_manufacturer_value,"
        "statecode,createdon"
    )
    
    return fetch_all(token, "lvp_quotelines", select, filter_query)


def format_quote_csv(quotes: List[dict]) -> str:
    """Format quotes as CSV for crosswalk."""
    output = StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow([
        "quote_number", "quote_id", "name", "status", "status_name",
        "extended_price", "probability", "fob", "freight_terms", "payment_terms",
        "customer_id", "customer_name", "salesrep",
        "created_date", "modified_date", "est_completion", "order_received_date"
    ])
    
    for q in quotes:
        # Get formatted customer name if available
        customer_name = ""
        customer_id = q.get("_lvp_customer_value", "")
        if customer_id:
            customer_name = q.get("_lvp_customer_value@OData.Community.Display.V1.FormattedValue", "")
        
        salesrep = q.get("_lvp_salesrep_value@OData.Community.Display.V1.FormattedValue", "")
        
        writer.writerow([
            q.get("lvp_quotenumber", ""),
            q.get("lvp_quoteid", ""),
            q.get("lvp_name", ""),
            q.get("lvp_quotestatus", ""),
            q.get("lvp_quotestatusname", ""),
            q.get("lvp_extendedprice", ""),
            q.get("lvp_probability", ""),
            q.get("lvp_fob", ""),
            q.get("lvp_freightterms", ""),
            q.get("lvp_paymentterms", ""),
            customer_id,
            customer_name,
            salesrep,
            q.get("createdon", ""),
            q.get("modifiedon", ""),
            q.get("lvp_estcompletiondate", ""),
            q.get("lvp_orderreceiveddate", ""),
        ])
    
    return output.getvalue()


def format_quote_lines_csv(lines: List[dict]) -> str:
    """Format quote lines as CSV for item matching."""
    output = StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow([
        "quote_number", "line_id", "item", "description", "quantity",
        "enpro_cost", "customer_net_price", "extended_price", "discount",
        "markup", "gross_margin", "list_price", "product_group",
        "quote_id", "manufacturer_id", "created_date"
    ])
    
    for line in lines:
        writer.writerow([
            line.get("lvp_quotenumber", ""),
            line.get("lvp_quotelineid", ""),
            line.get("lvp_item", ""),
            line.get("lvp_description", ""),
            line.get("lvp_quantity", ""),
            line.get("lvp_enprocost", ""),
            line.get("lvp_customernetpricepart", ""),
            line.get("lvp_extendedprice", ""),
            line.get("lvp_discount", ""),
            line.get("lvp_markup", ""),
            line.get("lvp_grossmargin", ""),
            line.get("lvp_listprice", ""),
            line.get("lvp_productgroup", ""),
            line.get("_lvp_quote_value", ""),
            line.get("_lvp_manufacturer_value", ""),
            line.get("createdon", ""),
        ])
    
    return output.getvalue()


def export_quotes_to_blob(days_back: int = 365) -> dict:
    """Main export function — pulls quotes from Dynamics, uploads to blob."""
    logger.info(f"Starting quote export (last {days_back} days)")
    
    token = load_token()
    
    # Fetch data
    logger.info("Fetching quotes...")
    quotes = get_recent_quotes(token, days_back)
    
    logger.info("Fetching quote lines...")
    lines = get_quote_lines(token, days_back)
    
    # Format as CSV
    quotes_csv = format_quote_csv(quotes)
    lines_csv = format_quote_lines_csv(lines)
    
    # Upload to blob
    uploader = get_uploader()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    results = {
        "timestamp": timestamp,
        "quotes_count": len(quotes),
        "lines_count": len(lines),
        "uploads": {}
    }
    
    if uploader.is_configured():
        # Upload quotes CSV
        quotes_blob = f"crosswalk/quotes/quotes_dynamics_{timestamp}.csv"
        from io import BytesIO
        
        try:
            quotes_bytes = quotes_csv.encode('utf-8')
            blob_client = uploader.client.get_blob_client(
                container=uploader.BLOB_CONTAINER_NAME,
                blob=quotes_blob
            )
            blob_client.upload_blob(BytesIO(quotes_bytes), overwrite=True)
            results["uploads"]["quotes"] = {"blob": quotes_blob, "success": True}
            logger.info(f"Uploaded quotes to {quotes_blob}")
        except Exception as e:
            logger.error(f"Failed to upload quotes: {e}")
            results["uploads"]["quotes"] = {"error": str(e), "success": False}
        
        # Upload lines CSV
        lines_blob = f"crosswalk/quotes/quote_lines_dynamics_{timestamp}.csv"
        try:
            lines_bytes = lines_csv.encode('utf-8')
            blob_client = uploader.client.get_blob_client(
                container=uploader.BLOB_CONTAINER_NAME,
                blob=lines_blob
            )
            blob_client.upload_blob(BytesIO(lines_bytes), overwrite=True)
            results["uploads"]["lines"] = {"blob": lines_blob, "success": True}
            logger.info(f"Uploaded quote lines to {lines_blob}")
        except Exception as e:
            logger.error(f"Failed to upload lines: {e}")
            results["uploads"]["lines"] = {"error": str(e), "success": False}
        
        # Also update "latest" versions
        try:
            blob_client = uploader.client.get_blob_client(
                container=uploader.BLOB_CONTAINER_NAME,
                blob="crosswalk/quotes/quotes_dynamics_latest.csv"
            )
            blob_client.upload_blob(BytesIO(quotes_bytes), overwrite=True)
            
            blob_client = uploader.client.get_blob_client(
                container=uploader.BLOB_CONTAINER_NAME,
                blob="crosswalk/quotes/quote_lines_dynamics_latest.csv"
            )
            blob_client.upload_blob(BytesIO(lines_bytes), overwrite=True)
            
            results["uploads"]["latest"] = {"success": True}
            logger.info("Updated latest versions")
        except Exception as e:
            logger.error(f"Failed to update latest: {e}")
    else:
        # Save locally if blob not configured
        local_dir = "./quote_exports"
        os.makedirs(local_dir, exist_ok=True)
        
        with open(f"{local_dir}/quotes_{timestamp}.csv", "w") as f:
            f.write(quotes_csv)
        with open(f"{local_dir}/quote_lines_{timestamp}.csv", "w") as f:
            f.write(lines_csv)
        
        results["local_path"] = local_dir
        logger.info(f"Saved locally to {local_dir}")
    
    logger.info(f"Export complete: {len(quotes)} quotes, {len(lines)} lines")
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Export Dynamics quotes to blob")
    parser.add_argument("--days", type=int, default=365, help="Days back to fetch")
    parser.add_argument("--local", action="store_true", help="Save locally only")
    args = parser.parse_args()
    
    result = export_quotes_to_blob(args.days)
    print(json.dumps(result, indent=2))
