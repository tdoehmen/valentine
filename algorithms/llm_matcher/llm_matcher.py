from algorithms.base_matcher import BaseMatcher
from data_loader.instance_loader import InstanceLoader
import openai
import json

class LLMMatcher(BaseMatcher):
    """
    Class containing the methods for implementing a simple baseline matcher that uses LLMs
    to assess their correspondence score.

    Methods
    -------
    llm_match(list1, list2)

    """

    def __init__(self):
        """
        Parameters
        ----------
        """
        #openai.api_key = ""

    def get_matches(self, source: InstanceLoader, target: InstanceLoader, dataset_name: str):

        src_table = source.table.name.replace("_source", "")
        trg_table = target.table.name.replace("_target", "")
        src_columns = source.table.columns.items()
        trg_columns = target.table.columns.items()

        prompt_template = "You are given the following SQL database tables: " \
                 "\n{tables}\n" \
                 "Output a json list with the following schema " \
                 "{{table, column, referencedTable, referencedColumn}} " \
                 "that contains all foreign key relationships in alphabetical order."

        table_template = "{table}({columns})"
        src_table_str = table_template.format(table=src_table, columns=", ".join([col[0] for col in src_columns]))
        trg_table_str = table_template.format(table=trg_table, columns=", ".join([col[0] for col in trg_columns]))

        prompt = prompt_template.format(tables=src_table_str + "\n" + trg_table_str)

        print(prompt)

        foreign_keys_llm = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that always returns responses in JSON without any additional explanations. Only Respond with the desired JSON, NOTHING else, no explanations."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )["choices"][0]["message"]["content"].strip("\n")

        print(foreign_keys_llm)

        try:
            fks = json.loads(foreign_keys_llm)
        except Exception as e:
            print(e)
            fks = {}

        matches = {}
        for fk in fks:
            if any([val is None for val in fk.values()]):
                continue
            matches[(fk["table"]+"_source", fk["column"]), (fk["referencedTable"]+"_target", fk["referencedColumn"])] = 1.0

        return matches
