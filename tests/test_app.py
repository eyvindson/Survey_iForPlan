import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app


class SurveyAppTest(unittest.TestCase):
    def test_default_config_matches_planned_matrix(self):
        config = app.load_config()

        self.assertEqual(len(config.options), 32)
        self.assertEqual(len(config.services), 25)
        self.assertTrue(config.options["option_id"].is_unique)
        self.assertTrue(config.services["service_id"].is_unique)
        self.assertEqual(
            config.services.groupby("service_group").size().to_dict(),
            {
                "Provisioning": 4,
                "Regulating and maintenance": 16,
                "Cultural": 5,
            },
        )
        self.assertEqual(
            config.options.groupby("csf_category").size().to_dict(),
            {
                "Landscape Restoration and Carbon Enhancement": 6,
                "Landscape planning": 3,
                "Carbon forestry": 7,
                "Soil management and fertility": 4,
                "Fire and fuel management": 4,
                "Adaptive silviculture and species selection for resilience": 8,
            },
        )

    def test_response_roundtrip_and_exports(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "survey.db"
            config = app.load_config()
            app.init_db(db_path=db_path)
            conn = app.connect_db(db_path)

            token = "demo-expert"
            option_id = config.options.iloc[0]["option_id"]
            service_id = config.services.iloc[0]["service_id"]
            service_label = config.services.iloc[0]["service_label"]

            app.upsert_response(conn, token, option_id, "importance", None, "5")
            app.upsert_response(conn, token, option_id, "feasibility", None, "Skip")
            app.upsert_response(conn, token, option_id, "effect", service_id, "Positive")
            conn.commit()

            tidy = app.build_tidy_export(conn, config.options, config.services)
            matrix = app.build_matrix_export(conn, config.options, config.services)

            option_rows = tidy[tidy["option_id"] == option_id]
            self.assertEqual(set(option_rows["answer"]), {"5", "skipped", "positive"})

            matrix_row = matrix[
                (matrix["token"] == token)
                & (matrix["Management option"] == config.options.iloc[0]["management_option"])
            ].iloc[0]
            self.assertEqual(matrix_row["Importance"], "5")
            self.assertEqual(matrix_row["Feasibility"], "skipped")
            self.assertEqual(matrix_row[service_label], "positive")

            conn.close()

    def test_matrix_export_isolated_by_expert(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "survey.db"
            config = app.load_config()
            app.init_db(db_path=db_path)
            conn = app.connect_db(db_path)

            option_id = config.options.iloc[0]["option_id"]
            app.upsert_response(conn, "demo-expert", option_id, "importance", None, "5")
            app.upsert_response(conn, "regional-planner", option_id, "importance", None, "2")
            conn.commit()

            matrix = app.build_matrix_export(conn, config.options, config.services)
            option_label = config.options.iloc[0]["management_option"]
            demo_row = matrix[
                (matrix["token"] == "demo-expert")
                & (matrix["Management option"] == option_label)
            ].iloc[0]
            planner_row = matrix[
                (matrix["token"] == "regional-planner")
                & (matrix["Management option"] == option_label)
            ].iloc[0]

            self.assertEqual(demo_row["Importance"], "5")
            self.assertEqual(planner_row["Importance"], "2")
            conn.close()

    def test_completion_counts_rating_skip_and_effect_answers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "survey.db"
            config = app.load_config()
            app.init_db(db_path=db_path)
            conn = app.connect_db(db_path)

            token = "demo-expert"
            option_id = config.options.iloc[0]["option_id"]
            for question in app.RATING_QUESTIONS:
                app.upsert_response(conn, token, option_id, question["question_id"], None, "Skip")
            for service_id in config.services["service_id"]:
                app.upsert_response(conn, token, option_id, "effect", service_id, "Neutral")
            conn.commit()

            status = app.completion_status(
                config.options, config.services, app.fetch_responses(conn, token)
            )
            first = status[status["option_id"] == option_id].iloc[0]
            self.assertEqual(first["answered"], 28)
            self.assertTrue(bool(first["complete"]))

            conn.close()


if __name__ == "__main__":
    unittest.main()
