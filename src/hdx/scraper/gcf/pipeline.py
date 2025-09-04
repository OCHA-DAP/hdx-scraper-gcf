#!/usr/bin/python
"""Gcf scraper"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
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
        self._countries = set()

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
            hxltags={},
            folder=self._tempdir,
            filename=f"{self._configuration['resource_activities']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )

        return dataset

    def generate_activities_by_country_dataset(
        self, iso3: str, country_data: List
    ) -> Optional[Dataset]:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_title = f"{country_name} - GCF Funded Activities"
        dataset_name = slugify(dataset_title)
        min_date, max_date = self._get_date_range(country_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_country_location(iso3)
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(startdate=min_date, enddate=max_date)

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_activities"],
            "description": f"{self._configuration['description_activities']} in {country_name}",
        }

        dataset.generate_resource_from_iterable(
            headers=list(country_data[0].keys()),
            iterable=country_data,
            hxltags={},
            folder=self._tempdir,
            filename=f"{dataset_name}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )

        return dataset

    def generate_countries_dataset(self) -> Optional[Dataset]:
        # Countries
        countries_data = self._get_countries_data()
        dataset_title = self._configuration["title_countries"]
        dataset_name = slugify(dataset_title)
        min_date, max_date = self._get_date_range(countries_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(min_date, max_date)

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_countries"],
            "description": self._configuration["description_countries"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(countries_data[0].keys()),
            iterable=countries_data,
            hxltags={},
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
        min_date, max_date = self._get_date_range(entities_data)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period(min_date, max_date)

        # Add resources here
        resource_data = {
            "name": self._configuration["resource_entities"],
            "description": self._configuration["description_entities"],
        }

        dataset.generate_resource_from_iterable(
            headers=list(entities_data[0].keys()),
            iterable=entities_data,
            hxltags={},
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
            hxltags={},
            folder=self._tempdir,
            filename=f"{self._configuration['resource_readiness']}.csv",
            resourcedata=resource_data,
            quickcharts=None,
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

    def _get_activities_data(self) -> List:
        """
        Get list of funded activities
        Generate columns according to table: https://data.greenclimate.fund/public/data/projects
        """
        records = self._fetch_project_data()

        activity_data = []
        for record in records:
            # Get entity name from first entity in list
            entities = record.get("Entities", [])
            entity = entities[0].get("Acronym") if entities else None

            # Get comma delimited string of country names from countries list
            countries = record.get("Countries", [])
            names = [c.get("CountryName") for c in countries if "CountryName" in c]
            country_names = ", ".join(names)

            # Get comma delimited string of country codes from countries list
            codes = [c.get("ISO3") for c in countries if "ISO3" in c]
            self._countries.update(codes)
            country_codes = ", ".join(codes)

            # Parse dates
            approval_date = self._format_date(record.get("ApprovalDate"))
            completion_date = self._format_date(record.get("DateCompletion"))

            # Determine modality
            ref_id = record.get("ApprovedRef", "")
            modality = (
                "FP"
                if ref_id.startswith("FP")
                else "SAP"
                if ref_id.startswith("SAP")
                else ""
            )

            # Get comma delimited string of result areas that are > 0%
            result_areas = record.get("ResultAreas", [])
            areas = [
                a.get("Area")
                for a in result_areas
                if a.get("Value") and float(a["Value"].rstrip("%")) > 0
            ]
            result_areas = ", ".join(areas)

            activity_data.append(
                {
                    "Ref #": ref_id,
                    "Modality": modality,
                    "Project Name": record.get("ProjectName"),
                    "Entity": entity,
                    "Countries": country_names,
                    "Country Codes": country_codes,
                    "Board Meeting": record.get("BoardMeeting", ""),
                    "Sector": record.get("Sector", ""),
                    "Theme": record.get("Theme", ""),
                    "Project Size": record.get("Size", ""),
                    "Approval Date": approval_date,
                    "Completion Date": completion_date,
                    "ESS Category": record.get("RiskCategory", ""),
                    "FA Financing": record.get("TotalGCFFunding", ""),
                    "Result Areas": result_areas,
                    "Status": record.get("Status", ""),
                    "Project URL": record.get("ProjectURL", ""),
                    "API URL": f"http://api.gcfund.org/v1/projects/{record.get('ProjectsID', '')}",
                }
            )

        return activity_data

    def _get_countries_data(self) -> List:
        """
        Get list of countries from project data
        Generate columns according to table: https://data.greenclimate.fund/public/data/countries

        "FA Financing" is financing for each project weighted by the “Country Allocation” (for multi-country
        projects we assign a country allocation, the sum of the allocation for each project is 1)
        """
        records = self._fetch_project_data()

        aggregated_data = {}
        for record in records:
            countries = record.get("Countries", [])
            project_funding = record.get("TotalGCFFunding", 0) or 0
            project_funding = project_funding / len(countries)
            approval_date = self._format_date(record.get("ApprovalDate"))
            for country in countries:
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
                        "Approval Date": approval_date,
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
            fa_financing = record.get("TotalGCFFunding", 0) or 0
            approval_date = self._format_date(record.get("ApprovalDate"))
            for entity in record.get("Entities", []):
                acronym = entity.get("Acronym")
                if not acronym:
                    continue

                if acronym not in aggregated_data:
                    aggregated_data[acronym] = {
                        "Entity": entity.get("Acronym", ""),
                        "Name": entity.get("Name", ""),
                        "DAE": "TRUE" if entity.get("Access") == "Direct" else "FALSE",
                        "Type": entity.get("Type", ""),
                        "Sector": entity.get("Sector"),
                        "# Approved": 0,
                        "FA Financing": 0,
                        "Approval Date": approval_date,
                    }

                # update aggregates
                agg_entry = aggregated_data[acronym]
                agg_entry["# Approved"] += 1
                agg_entry["FA Financing"] += fa_financing

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
            approval_date = self._format_date(rec.get("AgreementSignedDate"))

            result.append(
                {
                    "Ref #": rec.get("AgreementReference"),
                    "Activity": rec.get("Activity"),
                    "Project Title": rec.get("ProjectTitle"),
                    "Country": country_name,
                    "Delivery Partner": rec.get("DeliveryPartner"),
                    "Region": rec.get("Region"),
                    "Status": rec.get("Status"),
                    "Approval Date": approval_date,
                    "Financing": rec.get("AmountApprovedInUSD"),
                }
            )

        return result

    def get_activities_by_country(self) -> dict:
        """
        Group activity data by country
        """
        projects = self._get_activities_data()
        country_projects = defaultdict(list)
        for project in projects:
            # Split on commas in cases where project has multiple countries
            countries_data = project.get("Country Codes", "")
            countries = [
                code.strip() for code in countries_data.split(",") if code.strip()
            ]
            for country in countries:
                country_projects[country].append(project)
        return dict(country_projects)

    def _format_date(self, date: Optional[str]) -> Optional[str]:
        date_fmt = None
        if date:
            # Strip Z and format
            d = datetime.fromisoformat(date.replace("Z", "+00:00"))
            date_fmt = f"{d.strftime('%B')} {d.day}, {d.year}"
        return date_fmt

    def _get_date_range(self, records: List) -> Tuple:
        dates = []
        for rec in records:
            date = rec.get("Approval Date")
            if not date:
                continue
            dates.append(parse_date(date))

        if not dates:
            return None, None

        return min(dates), max(dates)
