#!/usr/bin/env python3
"""
REPSPHERES PROVIDER INTELLIGENCE ENRICHMENT ENGINE
Continuously enriches providers from rpin_providers table using FREE search APIs
"""

import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import json
import random
import re
from urllib.parse import quote
from supabase import create_client, Client
import hashlib

# Environment variables - MUST be set in Railway
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set as environment variables")
    exit(1)

class ProviderIntelligenceEngine:
    """
    Enriches actual providers from database with real intelligence
    """

    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.session = None
        self.cycle_count = 0
        self.providers_enriched = 0
        self.hot_leads_found = 0

        # Equipment signals to search for
        self.equipment_signals = [
            "Morpheus8", "CO2 laser", "IPL", "CoolSculpting", "Hydrafacial",
            "Ultherapy", "Clear + Brilliant", "Fraxel", "PicoSure", "Emsculpt",
            "InMode", "Candela", "Lumenis", "Sciton", "Cutera"
        ]

        # Expansion signals
        self.expansion_signals = [
            "new location", "grand opening", "now offering", "expanding",
            "hiring", "join our team", "second location", "new office"
        ]

        # Financial distress signals
        self.distress_signals = [
            "closing", "closed permanently", "out of business", "bankruptcy",
            "for sale", "shutting down"
        ]

    async def start(self):
        """Start the continuous enrichment engine"""
        print("="*80)
        print("🚀 REPSPHERES PROVIDER INTELLIGENCE ENGINE")
        print("="*80)
        print(f"Database: {SUPABASE_URL}")
        print(f"Started: {datetime.now()}")
        print("="*80)

        self.session = aiohttp.ClientSession(
            headers={'User-Agent': 'Mozilla/5.0 (compatible)'}
        )

        try:
            while True:
                self.cycle_count += 1
                await self.enrich_cycle()
                print(f"\n⏰ Waiting 5 minutes before next cycle...")
                await asyncio.sleep(300)  # 5 minutes - MORE AGGRESSIVE!

        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
        finally:
            if self.session:
                await self.session.close()

    async def enrich_cycle(self):
        """Run one enrichment cycle"""
        print(f"\n[CYCLE #{self.cycle_count}] {datetime.now().strftime('%H:%M:%S')}")
        print("-" * 60)

        # Get providers to enrich
        providers = await self.get_providers_to_enrich()

        if not providers:
            print("⚠️ No providers to enrich")
            return

        print(f"📊 Found {len(providers)} providers to enrich")

        # Process in parallel batches
        batch_size = 10
        for i in range(0, len(providers), batch_size):
            batch = providers[i:i+batch_size]
            tasks = [self.enrich_provider(p) for p in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    print(f"   ❌ Error: {result}")

            # Rate limiting
            await asyncio.sleep(2)

        print(f"\n✅ Cycle complete - Enriched: {self.providers_enriched}, Hot leads: {self.hot_leads_found}")

    async def get_providers_to_enrich(self):
        """Get providers from rpin_providers that need enrichment"""
        try:
            # Get providers that haven't been enriched recently
            response = self.supabase.table("rpin_providers").select(
                "id,npi,provider_name,first_name,last_name,organization_name,city,state"
            ).in_("state", ["CA", "MA", "PA", "TX", "FL", "NY"]).limit(100).execute()

            providers = response.data

            # Filter out already enriched providers (check rpin_provider_intelligence by NPI)
            enriched_response = self.supabase.table("rpin_provider_intelligence").select("npi").execute()
            enriched_npis = {r['npi'] for r in enriched_response.data if r.get('npi')}

            # Return providers not yet enriched
            return [p for p in providers if p.get('npi') and p['npi'] not in enriched_npis][:50]  # Limit to 50 per cycle

        except Exception as e:
            print(f"❌ Error getting providers: {e}")
            return []

    async def enrich_provider(self, provider):
        """Enrich a single provider with intelligence"""
        try:
            provider_name = self._build_provider_name(provider)
            location = f"{provider.get('city', '')}, {provider.get('state', '')}"

            print(f"   🔍 Enriching: {provider_name} - {location}")

            intelligence = {
                'provider_id': provider['id'],
                'npi': provider.get('npi'),
                'display_name': provider_name,
                'equipment_data': {},
                'market_insights': {},
                'opportunity_score': 0
            }

            # Search for intelligence using multiple methods
            tasks = [
                self.search_duckduckgo(provider_name, location),
                self.search_reddit(provider_name, provider.get('organization_name')),
                self.search_google_careful(provider_name, location)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            signals_found = []
            for result in results:
                if isinstance(result, dict):
                    signals_found.extend(result.get('signals', []))

            # Analyze signals
            for signal in signals_found:
                # Check for equipment
                for equipment in self.equipment_signals:
                    if equipment.lower() in signal.lower():
                        intelligence['equipment_data'][equipment] = {
                            'detected': True,
                            'source': 'web_search',
                            'timestamp': datetime.now().isoformat()
                        }
                        intelligence['opportunity_score'] += 20

                # Check for expansion
                for expansion in self.expansion_signals:
                    if expansion.lower() in signal.lower():
                        intelligence['market_insights']['expansion_signal'] = True
                        intelligence['opportunity_score'] += 15

                # Check for distress
                for distress in self.distress_signals:
                    if distress.lower() in signal.lower():
                        intelligence['market_insights']['distress_signal'] = True
                        intelligence['opportunity_score'] -= 30

            # Save to database if we found intelligence
            if intelligence['opportunity_score'] > 0:
                await self.save_intelligence(intelligence, provider)
                self.providers_enriched += 1

                if intelligence['opportunity_score'] >= 30:
                    self.hot_leads_found += 1
                    print(f"      🔥 HOT LEAD! Score: {intelligence['opportunity_score']}")

        except Exception as e:
            print(f"   ❌ Error enriching {provider.get('provider_name')}: {e}")

    async def search_duckduckgo(self, provider_name, location):
        """Search DuckDuckGo instant API (no key required)"""
        try:
            query = f"{provider_name} {location} medical aesthetic dermatology"
            url = f"https://api.duckduckgo.com/?q={quote(query)}&format=json&no_html=1"

            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()

                    signals = []
                    # Check abstract and related topics
                    if data.get('Abstract'):
                        signals.append(data['Abstract'])

                    for topic in data.get('RelatedTopics', [])[:3]:
                        if isinstance(topic, dict) and topic.get('Text'):
                            signals.append(topic['Text'])

                    return {'source': 'duckduckgo', 'signals': signals}
        except:
            pass
        return {'source': 'duckduckgo', 'signals': []}

    async def search_reddit(self, provider_name, organization_name):
        """Search Reddit for mentions (public JSON API, no key)"""
        try:
            search_term = organization_name or provider_name
            if not search_term:
                return {'source': 'reddit', 'signals': []}

            # Reddit public JSON endpoint
            url = f"https://www.reddit.com/search.json?q={quote(search_term)}&limit=5&sort=new"

            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()

                    signals = []
                    for post in data.get('data', {}).get('children', [])[:3]:
                        post_data = post.get('data', {})
                        title = post_data.get('title', '')
                        selftext = post_data.get('selftext', '')[:200]
                        signals.append(f"{title} {selftext}")

                    return {'source': 'reddit', 'signals': signals}
        except:
            pass
        return {'source': 'reddit', 'signals': []}

    async def search_google_careful(self, provider_name, location):
        """Carefully scrape Google search results"""
        try:
            query = f"{provider_name} {location} Morpheus8 CO2 laser reviews"
            url = f"https://www.google.com/search?q={quote(query)}"

            # Random delay to avoid detection
            await asyncio.sleep(random.uniform(1, 3))

            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()

                    signals = []
                    # Extract snippets (very basic, careful parsing)
                    import re
                    snippets = re.findall(r'<span[^>]*>([^<]{50,300})</span>', html)

                    for snippet in snippets[:5]:
                        if provider_name.split()[0].lower() in snippet.lower():
                            signals.append(snippet)

                    return {'source': 'google', 'signals': signals}
        except:
            pass
        return {'source': 'google', 'signals': []}

    async def save_intelligence(self, intelligence, provider):
        """Save enriched intelligence to database"""
        try:
            # Generate rpin_id (unique identifier for this intelligence record)
            npi_val = provider.get('npi')
            if npi_val and str(npi_val) != 'nan' and str(npi_val) != 'None':
                id_part = str(npi_val)
            else:
                id_part = str(provider['id'])[:8]
            rpin_id = f"RPIN-{id_part}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            # Prepare record for rpin_provider_intelligence
            record = {
                'rpin_id': rpin_id,  # REQUIRED field that was missing!
                'provider_id': None,  # provider_id expects UUID but we have integer IDs
                'npi': provider.get('npi'),
                'display_name': intelligence['display_name'],
                'first_name': provider.get('first_name'),
                'last_name': provider.get('last_name'),
                'organization_name': provider.get('organization_name'),
                'city': provider.get('city'),
                'state': provider.get('state'),
                'equipment_data': intelligence['equipment_data'] if intelligence['equipment_data'] else {},
                'market_insights': intelligence['market_insights'] if intelligence['market_insights'] else {},
                'opportunity_score': intelligence['opportunity_score'],
                'data_source': 'free_search_apis',
                'verified': False
            }

            # Insert to rpin_provider_intelligence (upsert not working)
            try:
                response = self.supabase.table("rpin_provider_intelligence").insert(record).execute()
            except Exception as e:
                print(f"      ⚠️ Insert failed, trying update: {str(e)[:100]}")
                # If exists, update instead
                response = self.supabase.table("rpin_provider_intelligence").update(record).eq('rpin_id', rpin_id).execute()

            # If high opportunity score, also add to provider_buying_signals
            if intelligence['opportunity_score'] >= 30:
                signal_record = {
                    'provider_id': provider['id'],
                    'signal_type': 'equipment_adoption' if intelligence['equipment_data'] else 'expansion',
                    'signal_strength': 'high' if intelligence['opportunity_score'] >= 50 else 'medium',
                    'details': json.dumps(intelligence),
                    'created_at': datetime.now().isoformat()
                }

                self.supabase.table("provider_buying_signals").insert(signal_record).execute()

            print(f"      ✅ Saved intelligence (score: {intelligence['opportunity_score']})")

        except Exception as e:
            print(f"      ❌ Error saving intelligence: {e}")

    def _build_provider_name(self, provider):
        """Build a searchable provider name"""
        if provider.get('organization_name'):
            return provider['organization_name']

        first = provider.get('first_name', '')
        last = provider.get('last_name', '')

        if first and last:
            return f"Dr. {first} {last}"
        elif last:
            return f"Dr. {last}"
        elif provider.get('provider_name'):
            return provider['provider_name']

        return "Unknown Provider"

async def main():
    """Main entry point"""
    engine = ProviderIntelligenceEngine()
    await engine.start()

if __name__ == "__main__":
    print("🚀 STARTING REPSPHERES PROVIDER INTELLIGENCE ENGINE")
    print("   • Pulling providers from rpin_providers table")
    print("   • Searching for equipment & expansion signals")
    print("   • Enriching rpin_provider_intelligence")
    print("   • Using FREE APIs only (no keys required)")
    print("   • Deploying to Railway...")

    asyncio.run(main())