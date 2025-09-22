#!/usr/bin/env python3
"""
TEST VERSION - Run one enrichment cycle
"""

import os
import asyncio
import aiohttp
from datetime import datetime
import json
import sys

# Add parent dir to path
sys.path.append('/home/jason/repos/repspheres-enrichment')
from app import ProviderIntelligenceEngine

async def test_run():
    """Run a single test cycle"""
    engine = ProviderIntelligenceEngine()

    print("🚀 TEST RUN - SINGLE ENRICHMENT CYCLE")
    print("="*60)

    engine.session = aiohttp.ClientSession(
        headers={'User-Agent': 'Mozilla/5.0 (compatible)'}
    )

    try:
        # Run one cycle
        await engine.enrich_cycle()
        print(f"\n✅ TEST COMPLETE")
        print(f"   • Providers enriched: {engine.providers_enriched}")
        print(f"   • Hot leads found: {engine.hot_leads_found}")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if engine.session:
            await engine.session.close()

if __name__ == "__main__":
    asyncio.run(test_run())