import json
from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader

class LLMMatcherReplay(BaseMatcher):
    """
    Class containing the methods for implementing a schema-based join detection method
    using LLM. Predictions will be replayed from a *model_type*_responses.json file that
    was created independently and contains the predictions of the LLM for each dataset.

    Methods
    -------
    get_matches(list1, list2, threshold, process_pool)

    """

    def __init__(self, replay_file: str):
        """
        Parameters
        ----------
        replay_file : float, optional
            json file containing a single prediction of the LLM for each dataset, e.g.:
             {"college_completion_sqlite_state_sector_grads_institution_details":
                {"table": "state_sector_grads",
                "column": "stateid",
                "referencedTable": "institution_details",
                "referencedColumn": "state"}
             }
        """
        self.replay_dict = json.load(open(replay_file, "r"))

    def get_original_column_name(self, column, columns_original):
        for col in columns_original:
            if column.lower() == col.lower():
                return col
        return None

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name: str):
        prediction = self.replay_dict[dataset_name]
        prediction_column = prediction["column"]
        prediction_referenced_column = prediction["referencedColumn"]

        col_src_name = self.get_original_column_name(prediction_column, source.table.columns.keys())
        col_trg_name = self.get_original_column_name(prediction_referenced_column, target.table.columns.keys())

        src_table = source.table.name
        trg_table = target.table.name

        matches = dict()
        if col_src_name and col_trg_name:
            matches[((src_table, col_src_name), (trg_table, col_trg_name))] = 1

        return matches
