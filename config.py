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
    graph_client_id: str = ""
    graph_client_secret: str = ""
    graph_tenant_id: str = ""
    graph_mailbox: str = "orders@enproinc.com"
    graph_poll_interval: int = 60

    # ── Dynamics 365 CRM ──
    dynamics_org_url: str = ""
    dynamics_client_id: str = ""
    dynamics_client_secret: str = ""
    dynamics_tenant_id: str = ""

    # ── CISM Output ──
    cism_output_dir: str = "./cism_output"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
