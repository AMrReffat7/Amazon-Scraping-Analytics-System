import asyncio
import aiohttp
import random
import json
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
import time
from urllib.parse import urljoin, urlencode

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AmazonLargeScraper:
    def __init__(self, base_url: str = "https://www.amazon.com"):
        self.base_url = base_url
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        self.semaphore = asyncio.Semaphore(3)

    def get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    async def fetch(
        self, session: aiohttp.ClientSession, url: str, retries: int = 3
    ) -> Optional[str]:
        async with self.semaphore:
            for i in range(retries):
                try:
                    await asyncio.sleep(random.uniform(2, 5))
                    async with session.get(
                        url, headers=self.get_headers(), timeout=20
                    ) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 404:
                            logger.warning(f"404 Not Found: {url}")
                            return None
                        elif response.status == 503:
                            logger.warning(
                                f"503 Service Unavailable (likely CAPTCHA/Block) for {url}. Retry {i+1}/{retries}"
                            )
                        else:
                            logger.warning(
                                f"Status {response.status} for {url}. Retry {i+1}/{retries}"
                            )
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}. Retry {i+1}/{retries}")

                await asyncio.sleep(2**i)
            return None

    def parse_search_results(self, html: str, keyword: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        products = []

        items = soup.find_all("div", {"data-asin": True})

        for item in items:
            asin = item.get("data-asin")
            if not asin:
                continue

            title_tag = item.find("h2")
            title = title_tag.get_text(strip=True) if title_tag else None

            price_tag = item.find("span", class_="a-offscreen")
            price = price_tag.get_text(strip=True) if price_tag else None

            rating_tag = item.find("span", class_="a-icon-alt")
            rating = (
                rating_tag.get_text(strip=True).split(" ")[0] if rating_tag else None
            )

            review_count_tag = item.find(
                "span",
                class_="a-size-base",
                string=lambda x: x and x.replace(",", "").isdigit(),
            )
            review_count = (
                review_count_tag.get_text(strip=True).replace(",", "")
                if review_count_tag
                else None
            )

            link_tag = item.find("a", class_="a-link-normal s-no-outline")
            link = urljoin(self.base_url, link_tag.get("href")) if link_tag else None

            if title:
                products.append(
                    {
                        "keyword": keyword,
                        "asin": asin,
                        "title": title,
                        "price": price,
                        "rating": rating,
                        "review_count": review_count,
                        "url": link,
                    }
                )

        return products

    async def scrape_keyword_pages(
        self, keyword: str, max_pages: int = 5
    ) -> List[Dict]:
        all_products_for_keyword = []
        async with aiohttp.ClientSession() as session:
            for page in range(1, max_pages + 1):
                logger.info(f"Scraping {keyword} - Page {page}")
                params = {"k": keyword, "page": page, "ref": f"sr_pg_{page}"}
                url = f"{self.base_url}/s?{urlencode(params)}"

                html = await self.fetch(session, url)
                if not html:
                    logger.error(f"Failed to retrieve page {page} for {keyword}")
                    break

                products = self.parse_search_results(html, keyword)  # Pass keyword here
                if not products:
                    logger.info(
                        f"No more products found on page {page} for keyword {keyword}"
                    )
                    break

                all_products_for_keyword.extend(products)
                logger.info(
                    f"Found {len(products)} products on page {page} for keyword {keyword}"
                )

                if "s-pagination-next" not in html and page < max_pages:
                    logger.info("No next page button found. Stopping for this keyword.")
                    break

        return all_products_for_keyword

    async def scrape_product_details(self, products: List[Dict]) -> List[Dict]:
        """Optional: Scrape full details for each product found in search"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for p in products:
                if p.get("url"):
                    tasks.append(self.fetch_and_parse_detail(session, p))

            return await asyncio.gather(*tasks)

    async def fetch_and_parse_detail(
        self, session: aiohttp.ClientSession, product: Dict
    ) -> Dict:
        html = await self.fetch(session, product["url"])
        if not html:
            return product

        soup = BeautifulSoup(html, "html.parser")

        brand_tag = soup.find("a", id="bylineInfo")
        product["brand"] = (
            brand_tag.get_text(strip=True)
            .replace("Visit the ", "")
            .replace(" Store", "")
            if brand_tag
            else None
        )

        avail_tag = soup.find("div", id="availability")
        product["availability"] = (
            avail_tag.get_text(strip=True) if avail_tag else "Unknown"
        )

        bullets = soup.select("#feature-bullets ul li span")
        product["features"] = (
            [b.get_text(strip=True) for b in bullets] if bullets else []
        )

        return product


async def main():
    scraper = AmazonLargeScraper()

    # List of keywords to scrape
    keywords = [
        # Electronics
        "smart tv",
        "android tv box",
        "soundbar",
        "home theater system",
        "projector",
        "noise cancelling headphones",
        "portable speaker",
        "walkie talkie",
        # Computers & Accessories
        "graphics card",
        "motherboard",
        "ram memory",
        "cpu processor",
        "pc case",
        "gaming monitor",
        "drawing tablet",
        "ethernet cable",
        # Gaming
        "gaming desk",
        "racing wheel",
        "gaming mouse pad",
        "console cooling fan",
        "game capture card",
        # Home & Kitchen
        "pressure cooker",
        "food processor",
        "dish drying rack",
        "water dispenser",
        "storage containers",
        "bed sheets",
        "blackout curtains",
        # Furniture
        "bookshelf",
        "tv stand",
        "wardrobe closet",
        "coffee table",
        "bed frame",
        "mattress topper",
        # Fashion
        "formal shoes",
        "casual sneakers",
        "abaya",
        "pajama set",
        "swimwear",
        "belt",
        "sunglasses",
        "wallet",
        # Beauty & Personal Care
        "hair straightener",
        "curling iron",
        "face serum",
        "body lotion",
        "makeup brushes",
        "nail kit",
        "beard trimmer",
        # Health & Fitness
        "resistance bands",
        "pull up bar",
        "fitness tracker watch",
        "massager gun",
        "foam roller",
        # Baby Products
        "baby crib",
        "baby walker",
        "baby clothes set",
        "baby blanket",
        "pacifier",
        # Toys & Games
        "lego set",
        "board games",
        "puzzle 1000 pieces",
        "remote control car",
        "action figures",
        # Pet Supplies
        "cat tree",
        "dog crate",
        "pet grooming kit",
        "automatic pet feeder",
        "aquarium tank",
        # Office Products
        "desk organizer",
        "filing cabinet",
        "sticky notes",
        "label maker",
        "paper shredder",
        # Tools & Home Improvement
        "electric screwdriver",
        "angle grinder",
        "tool storage box",
        "wall drill bits",
        "paint roller",
        # Automotive
        "car jump starter",
        "car phone holder",
        "roof rack",
        "car polish kit",
        "led headlights",
        # Garden & Outdoor
        "garden hose",
        "outdoor furniture set",
        "bbq grill",
        "patio umbrella",
        "solar garden lights",
    ]
    max_pages_per_keyword = 3

    all_scraped_products = []

    for keyword in keywords:
        logger.info(f"Starting large-scale scrape for keyword: {keyword}")
        products_for_keyword = await scraper.scrape_keyword_pages(
            keyword, max_pages=max_pages_per_keyword
        )
        all_scraped_products.extend(products_for_keyword)
        logger.info(
            f"Finished scraping {len(products_for_keyword)} products for keyword: {keyword}"
        )

    logger.info(
        f"Total products found across all keywords: {len(all_scraped_products)}"
    )

    # Save results
    df = pd.DataFrame(all_scraped_products)
    df.to_csv("./data/amazon_multi_keyword_results.csv", index=False)
    with open("./data/amazon_multi_keyword_results.json", "w") as f:
        json.dump(all_scraped_products, f, indent=4)

    logger.info(
        "Data saved to ./data/amazon_multi_keyword_results.csv and ./data/amazon_multi_keyword_results.json"
    )


if __name__ == "__main__":
    asyncio.run(main())
