import pandas as pd


def load_data(data1, data2):
    dataset1 = pd.read_csv(data1)
    dataset2 = pd.read_csv(data2)
    return dataset1, dataset2


def join_datasets(dataset1, dataset2):
    """
    Join dataset1 with dataset2 on the counter_party column.

    Args:
        dataset1 (pd.DataFrame): DataFrame containing dataset1 data.
        dataset2 (pd.DataFrame): DataFrame containing dataset2 data.

    Returns:
        pd.DataFrame: A new DataFrame with the merged data.
    """
    merged_data = dataset1.merge(dataset2, on="counter_party")
    return merged_data


def calculate_values(merged_data):
    """
    Calculate ARAP and ACCR sums, and the max rating for each group.

    Args:
        merged_data (pd.DataFrame): DataFrame containing the merged data.

    Returns:
        pd.DataFrame: DataFrames containing ARAP sums, ACCR sums, and max ratings.
    """
    arap_sum = (
        merged_data.loc[merged_data["status"] == "ARAP"]
        .groupby(["legal_entity", "counter_party", "tier"])["value"]
        .sum()
        .reset_index()
    )
    arap_sum.columns = ["legal_entity", "counter_party", "tier", "sum(value where status=ARAP)"]

    accr_sum = (
        merged_data.loc[merged_data["status"] == "ACCR"]
        .groupby(["legal_entity", "counter_party", "tier"])["value"]
        .sum()
        .reset_index()
    )
    accr_sum.columns = ["legal_entity", "counter_party", "tier", "sum(value where status=ACCR)"]

    max_rating = merged_data.groupby(["legal_entity", "counter_party", "tier"])["rating"].max().reset_index()
    max_rating.columns = ["legal_entity", "counter_party", "tier", "max(rating by counterparty)"]

    return arap_sum, accr_sum, max_rating


def merge_calculated_data(arap_sum, accr_sum, max_rating):
    """
    Merge the calculated ARAP sums, ACCR sums, and max ratings into a single DataFrame.

    Args:
        arap_sum (pd.DataFrame): DataFrame containing ARAP sums.
        accr_sum (pd.DataFrame): DataFrame containing ACCR sums.
        max_rating (pd.DataFrame): DataFrame containing max ratings.

    Returns:
        pd.DataFrame: A new DataFrame containing the merged data.
    """
    result = max_rating.merge(arap_sum, on=["legal_entity", "counter_party", "tier"], how="left").merge(
        accr_sum, on=["legal_entity", "counter_party", "tier"], how="left"
    )
    result["max(rating by counterparty)"] = result["max(rating by counterparty)"].fillna(0)
    result["sum(value where status=ARAP)"] = result["sum(value where status=ARAP)"].fillna(0)
    result["sum(value where status=ACCR)"] = result["sum(value where status=ACCR)"].fillna(0)
    return result


def calculate_totals(result):
    """
    Calculate the totals for each of legal_entity, counter_party, and tier.

    Args:
        result (pd.DataFrame): DataFrame containing the merged data.

    Returns:
        pd.DataFrame: DataFrames containing the calculated totals for legal_entity, counter_party, and tier.
    """
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

    return legal_entity_total, counter_party_total, tier_total


def append_totals(result, legal_entity_total, counter_party_total, tier_total):
    """
    Append the calculated totals to the result DataFrame.

    Args:
        result (pd.DataFrame): DataFrame containing the merged data.
        legal_entity_total (pd.DataFrame): DataFrame containing the legal_entity totals.
        counter_party_total (pd.DataFrame): DataFrame containing the counter_party totals.
        tier_total (pd.DataFrame): DataFrame containing the tier totals.

    Returns:
        pd.DataFrame: A new DataFrame with the appended totals.
    """
    result = pd.concat([result, legal_entity_total, counter_party_total, tier_total], ignore_index=True)
    return result


def save_to_csv(result, filename="output.csv"):
    """
    Save the result DataFrame to a CSV file.

    vbnet
    Copy code
    Args:
        result (pd.DataFrame): DataFrame containing the merged data and totals.
        filename (str, optional): The name of the output CSV file. Defaults to 'output.csv'.
    """
    result.to_csv(f"./output_data/{filename}", index=False)


def main(data1, data2):
    dataset1, dataset2 = load_data(data1, data2)
    merged_data = join_datasets(dataset1, dataset2)
    arap_sum, accr_sum, max_rating = calculate_values(merged_data)
    result = merge_calculated_data(arap_sum, accr_sum, max_rating)
    legal_entity_total, counter_party_total, tier_total = calculate_totals(result)
    final_result = append_totals(result, legal_entity_total, counter_party_total, tier_total)
    save_to_csv(final_result)


if __name__ == "__main__":
    data1 = "./input_data/dataset1.csv"
    data2 = "./input_data/dataset2.csv"
    main(data1, data2)

