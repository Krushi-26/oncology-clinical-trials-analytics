from datetime import datetime
from dateutil import parser


class ClinicalTrialTransformer:

    @staticmethod
    def transform(api_response):

        studies = api_response.get("studies", [])
        transformed_records = []

        for study in studies:
            try:
                protocol = study.get("protocolSection", {})

                identification = protocol.get("identificationModule", {})
                status_module = protocol.get("statusModule", {})
                design_module = protocol.get("designModule", {})
                sponsor_module = protocol.get("sponsorCollaboratorsModule", {})
                conditions_module = protocol.get("conditionsModule", {})
                contacts_locations = protocol.get("contactsLocationsModule", {})

                # Basic Fields
                nct_id = identification.get("nctId")
                status = status_module.get("overallStatus")

                start_date = status_module.get("startDateStruct", {}).get("date")
                last_update = status_module.get("lastUpdatePostDateStruct", {}).get("date")

                phase = design_module.get("phases", [None])[0]
                enrollment = design_module.get("enrollmentInfo", {}).get("count")
                sponsor = sponsor_module.get("leadSponsor", {}).get("name")
                conditions = conditions_module.get("conditions", [])
                locations = contacts_locations.get("locations", [])

                cancer_type = conditions[0] if conditions else None
                state = locations[0].get("state") if locations else None

                # ---- Flexible Date Parsing ---- #

                if start_date:
                    try:
                        start_date = parser.parse(start_date)
                    except Exception:
                        start_date = None

                if last_update:
                    try:
                        last_update = parser.parse(last_update)
                    except Exception:
                        last_update = None

                # ---- Append Record ---- #

                transformed_records.append({
                    "nct_id": nct_id,
                    "cancer_type": cancer_type,
                    "phase": phase,
                    "status": status,
                    "start_date": start_date,
                    "enrollment": enrollment,
                    "sponsor_type": sponsor,
                    "state": state,
                    "last_update_posted": last_update
                })

            except Exception as e:
                print(f"⚠ Skipping record due to error: {e}")

        return transformed_records