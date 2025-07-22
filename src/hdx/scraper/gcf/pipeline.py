#!/usr/bin/python
"""Gcf scraper"""

import logging
from copy import deepcopy
from datetime import datetime
from typing import List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._base_url = self._configuration["base_url"]
        self._project_data = None
        self._readiness_data = None

    def generate_activities_dataset(self) -> Optional[Dataset]:
        # Funded Activities
        activities_data = self._get_activities_data()

        dataset_title = self._configuration["title_activities"]
        dataset_name = slugify(dataset_title)
        min_date, max_date = self._get_date_range(activities_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(startdate=min_date, enddate=max_date)

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_activities"],
            "description": self._configuration["description_activities"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(activities_data[0].keys()),
            iterable=activities_data,
            hxltags=self._configuration["hxl_tags_activities"],
            folder=self._tempdir,
            filename=f"{self._configuration['resource_activities']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )
        return dataset

    def generate_countries_dataset(self) -> Optional[Dataset]:
        # Countries
        countries_data = self._get_countries_data()
        dataset_title = self._configuration["title_countries"]
        dataset_name = slugify(dataset_title)
        # min_date, max_date = self._get_date_range(countries_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(startdate="January 1, 2024", enddate="January 1, 2025")

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_countries"],
            "description": self._configuration["description_countries"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(countries_data[0].keys()),
            iterable=countries_data,
            hxltags=self._configuration["hxl_tags_countries"],
            folder=self._tempdir,
            filename=f"{self._configuration['resource_countries']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )
        return dataset

    def generate_entities_dataset(self) -> Optional[Dataset]:
        # Entities
        entities_data = self._get_entities_data()

        dataset_title = self._configuration["title_entities"]
        dataset_name = slugify(dataset_title)
        # min_date, max_date = self._get_date_range(countries_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(startdate="January 1, 2024", enddate="January 1, 2025")

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_entities"],
            "description": self._configuration["description_entities"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(entities_data[0].keys()),
            iterable=entities_data,
            hxltags=self._configuration["hxl_tags_entities"],
            folder=self._tempdir,
            filename=f"{self._configuration['resource_entities']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )
        return dataset

    def generate_readiness_dataset(self) -> Optional[Dataset]:
        # Readiness
        readiness_data = self._get_readiness_data()

        dataset_title = self._configuration["title_readiness"]
        dataset_name = slugify(dataset_title)
        min_date, max_date = self._get_date_range(readiness_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(startdate=min_date, enddate=max_date)

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_readiness"],
            "description": self._configuration["description_readiness"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(readiness_data[0].keys()),
            iterable=readiness_data,
            hxltags=self._configuration["hxl_tags_readiness"],
            folder=self._tempdir,
            filename=f"{self._configuration['resource_readiness']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )
        return dataset

    def generate_by_country_dataset(self) -> Optional[Dataset]:
        countries = self.get_data_by_country(self._project_data)
        print(countries)

        dataset = Dataset(
            {
                "name": "dataset name",
                "title": "dataset title",
            }
        )

        return dataset

    def _fetch_project_data(self) -> List:
        if self._project_data is None:
            data_url = f"{self._base_url}/projects"
            self._project_data = self._retriever.download_json(data_url)
        return self._project_data

    def _fetch_readiness_data(self) -> List:
        if self._readiness_data is None:
            data_url = f"{self._base_url}/readinessProjects"
            self._readiness_data = self._retriever.download_json(data_url)
        return self._readiness_data

    def _filter_by_iso3(self, iso3_code):
        """
        Return a list of all rows where the 'ISO3' field equals iso3_code.
        """
        data = self._fetch_project_data()
        print(data)
        return [row for row in data if row.get("iso3") == iso3_code]

    def _get_activities_data(self) -> List:
        """
        Get list of funded activities
        Generate columns according to table: https://data.greenclimate.fund/public/data/projects
        """
        records = self._fetch_project_data()

        result = []
        for record in records:
            # Get entity name from first entity in list
            entities = record.get("Entities", [])
            entity = entities[0].get("Acronym") if entities else None

            # Get comma delimited string of country names from countries list
            countries = record.get("Countries", [])
            names = [c.get("CountryName") for c in countries if "CountryName" in c]
            country_names = ", ".join(names)

            # Parse date
            approved_date = record.get("ApprovalDate")
            approved_date_fmt = None
            if approved_date:
                # Strip Z and format
                date = datetime.fromisoformat(approved_date.replace("Z", "+00:00"))
                approved_date_fmt = f"{date.strftime('%B')} {date.day}, {date.year}"

            result.append(
                {
                    "Ref #": record.get("ApprovedRef"),
                    "Modality": "",
                    "Project Name": record.get("ProjectName"),
                    "Entity": entity,
                    "Countries": country_names,
                    "BM": record.get("BoardMeeting"),
                    "Sector": record.get("Sector"),
                    "Theme": record.get("Theme"),
                    "Project Size": record.get("Size"),
                    "Approved Date": approved_date_fmt,
                    "ESS Category": record.get("RiskCategory"),
                    "FA Financing": record.get("TotalGCFFunding"),
                    "Website": record.get("ProjectURL"),
                }
            )

        return result

    def _get_countries_data(self) -> List:
        """
        Get list of countries from project data
        Generate columns according to table: https://data.greenclimate.fund/public/data/countries
        """
        records = self._fetch_project_data()

        aggregated_data = {}
        for record in records:
            project_funding = record.get("TotalGCFFunding", 0) or 0
            for country in record.get("Countries", []):
                iso3 = country.get("ISO3")
                if not iso3:
                    continue

                if iso3 not in aggregated_data:
                    aggregated_data[iso3] = {
                        "ISO3": iso3,
                        "Country Name": country.get("CountryName", ""),
                        "Region": country.get("Region", ""),
                        "LDCs": country.get("LDCs", False),
                        "SIDS": country.get("SIDS", False),
                        "FA Financing": 0,
                        "# FA": 0,
                        "# RP": 0,
                        "RP Financing": 0,
                    }

                # update aggregates
                agg_entry = aggregated_data[iso3]
                agg_entry["FA Financing"] += project_funding
                agg_entry["# FA"] += 1

        return list(aggregated_data.values())

    def _get_entities_data(self) -> List:
        """
        Get list of entities from project data
        Generate columns according to table: https://data.greenclimate.fund/public/data/entities
        """
        records = self._fetch_project_data()

        aggregated_data = {}
        for record in records:
            for entity in record.get("Entities", []):
                acronym = entity.get("Acronym")
                name = entity.get("Name")
                if not (acronym or name):
                    continue

                if acronym not in aggregated_data:
                    aggregated_data[acronym] = {
                        "Entity": entity.get("Acronym", ""),
                        "Name": entity.get("Name", ""),
                        "Country": [],
                        "DAE": "TRUE" if entity.get("Access") == "Direct" else "FALSE",
                        "Type": entity.get("Type", ""),
                        "Stage": "",
                        "BM": record.get("BoardMeeting"),
                        "Size": record.get("Size"),
                        "Sector": entity.get("Sector"),
                        "# Approved": 0,
                        "FA Financing": 0,
                    }

                # update aggregates
                agg_entry = aggregated_data[acronym]
                agg_entry["# Approved"] += 1
                # countries = record.get("Countries")
                # names = []
                # for country in countries:
                #     names.append(country.get("CountryName"))
                # agg_entry["Country"] = ", ".join(names)

        return list(aggregated_data.values())

    def _get_readiness_data(self) -> List:
        """
        Get list of readiness projects
        Generate columns according to table: https://data.greenclimate.fund/public/data/readiness
        """
        records = self._fetch_readiness_data()

        result = []
        for rec in records:
            # Get country name from first country in list
            countries = rec.get("Countries", [])
            country_name = countries[0].get("CountryName") if countries else None

            # Parse date
            approved_date = rec.get("AgreementSignedDate")
            approved_date_fmt = None
            if approved_date:
                # Strip Z and format
                date = datetime.fromisoformat(approved_date.replace("Z", "+00:00"))
                approved_date_fmt = f"{date.strftime('%B')} {date.day}, {date.year}"

            result.append(
                {
                    "Ref #": rec.get("AgreementReference"),
                    "Activity": rec.get("Activity"),
                    "Project Title": rec.get("ProjectTitle"),
                    "Country": country_name,
                    "Delivery Partner": rec.get("DeliveryPartner"),
                    "Region": rec.get("Region"),
                    "Status": rec.get("Status"),
                    "Approved Date": approved_date_fmt,
                    "Financing": rec.get("AmountApprovedInUSD"),
                }
            )

        return result

    def get_data_by_country(self, projects):
        """
        Given a list of project dicts (each with a 'Countries' list),
        return a list of unique country dicts that include both
        the country's own fields and all project-level fields.
        """
        unique = {}

        for proj in projects:
            proj_id = proj.get("ProjectsID")
            proj_name = proj.get("ProjectName")
            for c in proj.get("Countries", []):
                iso = c["ISO3"]
                if iso not in unique:
                    # start with a deep copy so we don't accidentally mutate the original
                    country_copy = deepcopy(c)
                    # add an empty list for Projects
                    country_copy["Projects"] = []
                    unique[iso] = country_copy
                # append this project to the country's Projects list
                unique[iso]["Projects"].append(
                    {"ProjectsID": proj_id, "ProjectName": proj_name}
                )

        return list(unique.values())

    def _get_date_range(
        self, records: List
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        dates = []
        for rec in records:
            date = rec.get("Approved Date")
            if not date:
                continue
            dates.append(date)

        if not dates:
            return None, None

        return min(dates), max(dates)
