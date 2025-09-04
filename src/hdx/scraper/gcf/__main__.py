#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.gcf._version import __version__
from hdx.scraper.gcf.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-gcf"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Gcf"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)
            #
            # Steps to generate dataset
            #

            datasets = []
            tables = ["activities", "countries", "entities", "readiness"]

            for table in tables:
                func = getattr(pipeline, f"generate_{table}_dataset")
                datasets.append({"table": table, "dataset": func()})

            # Generate table based datasets
            for d in datasets:
                table = d["table"]
                dataset = d["dataset"]
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )

                    # Set metadata based on table
                    caveats_key = f"caveats_{table}"
                    dataset["caveats"] = configuration.get(caveats_key, "")

                    notes_key = f"notes_{table}"
                    dataset["notes"] = configuration.get(notes_key, "")

                    dataset.create_in_hdx(
                        remove_additional_resources=False,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )

            # Generate country specific activity datasets
            country_data = pipeline.get_activities_by_country()
            for iso3, country_activities in country_data.items():
                country_dataset = pipeline.generate_activities_by_country_dataset(
                    iso3, country_activities
                )

                if country_dataset:
                    country_dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )

                    # Set metadata
                    country_dataset["caveats"] = configuration.get(
                        "caveats_activities", ""
                    )
                    country_dataset["notes"] = configuration.get("notes_activities", "")

                    country_dataset.create_in_hdx(
                        remove_additional_resources=False,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )


if __name__ == "__main__":
    facade(
        main,
        # hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
