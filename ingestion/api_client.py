import requests


class ClinicalTrialsAPI:
    BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

    def fetch_oncology_trials(self, page_size=10):
        """
        Fetch oncology clinical trials
        """

        params = {
            "query.term": "cancer",
            "pageSize": page_size
        }

        try:
            response = requests.get(self.BASE_URL, params=params)

            if response.status_code == 200:
                print("✅ API fetch successful")
                return response.json()
            else:
                print(f"❌ API Error: {response.status_code}")
                print(response.text)  # print actual error
                return None

        except Exception as e:
            print(f"❌ Exception occurred: {e}")
            return None
