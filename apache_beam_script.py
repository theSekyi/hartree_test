import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions


def compute_aggregations(df):
    """Computes various aggregations on a given dataframe."""
    df = pd.DataFrame(df)
    arap_sum = (
        df[df["status"] == "ARAP"]
        .groupby(["legal_entity", "counter_party", "tier"])
        .agg({"value": "sum"})
        .reset_index()
        .rename(columns={"value": "sum(value where status=ARAP)"})
    )
    accr_sum = (
        df[df["status"] == "ACCR"]
        .groupby(["legal_entity", "counter_party", "tier"])
        .agg({"value": "sum"})
        .reset_index()
        .rename(columns={"value": "sum(value where status=ACCR)"})
    )
    max_rating = (
        df.groupby(["legal_entity", "counter_party", "tier"])
        .agg({"rating": "max"})
        .reset_index()
        .rename(columns={"rating": "max(rating by counterparty)"})
    )
    result = max_rating.merge(arap_sum, on=["legal_entity", "counter_party", "tier"], how="left").merge(
        accr_sum, on=["legal_entity", "counter_party", "tier"], how="left"
    )
    result["max(rating by counterparty)"] = result["max(rating by counterparty)"].fillna(0)
    result["sum(value where status=ARAP)"] = result["sum(value where status=ARAP)"].fillna(0)
    result["sum(value where status=ACCR)"] = result["sum(value where status=ACCR)"].fillna(0)
    return result


def compute_totals(result):
    """Computes totals of various aggregations for each legal entity, counter party and tier."""
    legal_entity_total = (
        result.groupby("legal_entity")
        .agg(
            {
                "max(rating by counterparty)": "sum",
                "sum(value where status=ARAP)": "sum",
                "sum(value where status=ACCR)": "sum",
            }
        )
        .reset_index()
    )
    legal_entity_total["counter_party"] = "Total"
    legal_entity_total["tier"] = "Total"

    counter_party_total = (
        result.groupby("counter_party")
        .agg(
            {
                "max(rating by counterparty)": "sum",
                "sum(value where status=ARAP)": "sum",
                "sum(value where status=ACCR)": "sum",
            }
        )
        .reset_index()
    )
    counter_party_total["legal_entity"] = "Total"
    counter_party_total["tier"] = "Total"

    tier_total = (
        result.groupby("tier")
        .agg(
            {
                "max(rating by counterparty)": "sum",
                "sum(value where status=ARAP)": "sum",
                "sum(value where status=ACCR)": "sum",
            }
        )
        .reset_index()
    )
    tier_total["legal_entity"] = "Total"
    tier_total["counter_party"] = "Total"

    result = pd.concat([result, legal_entity_total, counter_party_total, tier_total], ignore_index=True)
    return result


class ComputeAggregations(beam.PTransform):
    """Custom transform to compute aggregations using Pandas."""

    def expand(self, pcoll):
        return pcoll | beam.Map(compute_aggregations)


class ComputeTotals(beam.PTransform):
    """Custom transform to compute totals using Pandas."""

    def expand(self, pcoll):
        return pcoll | beam.Map(compute_totals)


def save_to_csv(dataframe):
    """Saves the given dataframe to CSV file."""
    dataframe.to_csv("./output_data/beam_output.csv", index=False)


def df_to_csv_string(dataframe):
    """Converts the given dataframe to a CSV string."""
    return dataframe.to_csv(index=False)


def run_pipeline():
    with beam.Pipeline(options=PipelineOptions()) as p:
        dataset1 = p | "Read dataset1" >> beam.io.ReadFromText("./input_data/dataset1.csv", skip_header_lines=1)
        dataset2 = p | "Read dataset2" >> beam.io.ReadFromText("./input_data/dataset2.csv", skip_header_lines=1)

        dataset1_df = dataset1 | "Convert dataset1 to DF" >> beam.Map(
            lambda line: pd.DataFrame([dict(zip(["legal_entity", "counter_party", "tier", "rating"], line.split(",")))])
        )
        dataset2_df = dataset2 | "Convert dataset2 to DF" >> beam.Map(
            lambda line: pd.DataFrame(
                [dict(zip(["legal_entity", "counter_party", "tier", "status", "value"], line.split(",")))]
            )
        )

        combined_df = (dataset1_df, dataset2_df) | "Flatten" >> beam.Flatten()
        aggregations = combined_df | "Compute aggregations" >> ComputeAggregations()
        totals = aggregations | "Compute totals" >> ComputeTotals()
        csv_string = totals | "Convert to CSV string" >> beam.Map(df_to_csv_string)
        csv_string | "Write to CSV" >> beam.io.WriteToText(
            "../output_data/beam_output.csv",
            header="legal_entity,counter_party,tier,max(rating by counterparty),sum(value where status=ARAP),sum(value where status=ACCR)",
            file_name_suffix=".csv",
            num_shards=1,
        )


if __name__ == "__main__":
    run_pipeline()

