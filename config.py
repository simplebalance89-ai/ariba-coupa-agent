"""
config.py — Environment configuration for Ariba/Coupa PO Automation Agent.
"""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── App ──
    app_name: str = "Ariba/Coupa PO Automation Agent"
    app_version: str = "1.0.0"
    environment: str = "development"
    debug: bool = False
    port: int = 8000

    # ── Azure SQL Staging DB ──
    staging_sql_server: str = ""
    staging_sql_database: str = "po_staging"
    staging_sql_driver: str = "{ODBC Driver 18 for SQL Server}"

    # ── Azure Blob Storage ──
    blob_account_url: str = ""
    blob_container: str = "ariba-coupa"
    blob_connection_string: str = ""

    # ── Azure Document Intelligence (PDF parsing) ──
    doc_intel_endpoint: str = ""
    doc_intel_key: str = ""

    # ── Azure OpenAI (optional — for AI-assisted field mapping) ──
    azure_openai_endpoint: str = ""
    azure_openai_key: str = ""
    azure_openai_model: str = "gpt-4o"

    # ── Microsoft Graph API (email polling) ──
    # Support both naming conventions (graph_* and AZURE_*)
    graph_client_id: str = ""
    graph_client_secret: str = ""
    graph_tenant_id: str = ""
    graph_mailbox: str = "orders@enproinc.com"
    graph_poll_interval: int = 60
    
    # Aliases for Azure AD credentials (used by email_poller)
    azure_tenant_id: str = ""
    azure_client_id: str = ""  
    azure_client_secret: str = ""

    # ── Dynamics 365 CRM ──
    dynamics_org_url: str = ""
    dynamics_client_id: str = ""
    dynamics_client_secret: str = ""
    dynamics_tenant_id: str = ""

    # ── P21 SQL (direct ODBC for SO pull / reads) ──
    p21_sql_server: str = ""
    p21_sql_database: str = "P21"
    p21_sql_driver: str = "{ODBC Driver 17 for SQL Server}"
    p21_sql_uid: str = ""
    p21_sql_pwd: str = ""
    p21_company_no: int = 1
    p21_location_id: int = 10

    # ── CISM Output ──
    cism_output_dir: str = "./cism_output"
    cism_so_output_dir: str = "./cism_so_output"

    # ── Crosswalk CSVs ──
    crosswalk_dir: str = "./crosswalks"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
