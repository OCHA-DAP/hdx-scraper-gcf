from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.gcf.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestGcf",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                datasets = []
                tables = ["activities"]  # , "countries", "entities", "readiness"

                for table in tables:
                    func = getattr(pipeline, f"generate_{table}_dataset")
                    datasets.append({"table": table, "dataset": func()})

                for d in datasets:
                    table = d["table"]
                    dataset = d["dataset"]
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )

                    assert dataset == {
                        "name": "global-climate-funded-activities",
                        "title": "Global Climate Funded Activities",
                        "dataset_date": "[2017-04-06T00:00:00 TO 2021-10-07T23:59:59]",
                        "tags": [
                            {
                                "name": "climate-weather",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "funding",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "license_id": "cc-by",
                        "methodology": "Registry",
                        "dataset_source": "Multiple sources",
                        "groups": [{"name": "world"}],
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                        "owner_org": "hdx",
                        "data_update_frequency": 30,
                    }

                    resources = dataset.get_resources()
                    assert resources == [
                        {
                            "name": "Climate funded activities",
                            "description": "A list of climate funded activities",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                    ]
