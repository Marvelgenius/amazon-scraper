import unittest

from src.brand_utils import clean_brand_name, extract_brand_from_payload


class BrandUtilsTests(unittest.TestCase):
    def test_extract_known_brand_from_title_prefix(self) -> None:
        payload = {
            "product_title": "OutIn Nano Portable Electric Espresso Machine for Camping",
        }
        self.assertEqual(extract_brand_from_payload(payload), "OutIn")

    def test_reject_generic_title_prefix(self) -> None:
        payload = {
            "product_title": "Portable Electric Espresso Machine Travel Coffee Maker",
        }
        self.assertIsNone(extract_brand_from_payload(payload))

    def test_preserve_multiword_brand_alias(self) -> None:
        payload = {
            "product_title": "Mr. Coffee® 5-Cup Mini Brew Switch Coffee Maker, Black",
        }
        self.assertEqual(extract_brand_from_payload(payload), "Mr. Coffee")

    def test_extract_brand_from_brand_colon_byline(self) -> None:
        payload = {
            "product_byline": "Brand: ExploreHorizon",
            "product_title": "14L/3.8Gallon Large Commercial Coffee Pot",
        }
        self.assertEqual(extract_brand_from_payload(payload), "ExploreHorizon")

    def test_extract_brand_from_store_byline(self) -> None:
        payload = {
            "product_byline": "Visit the minkah Store",
            "product_title": "Portable Coffee Maker Mug for Travel",
        }
        self.assertEqual(extract_brand_from_payload(payload), "minkah")

    def test_clean_brand_rejects_noise_token(self) -> None:
        self.assertIsNone(clean_brand_name("Mini"))
        self.assertIsNone(clean_brand_name("3-in-1"))


if __name__ == "__main__":
    unittest.main()
