# utils/validation_utils.py

def perform_validation(df, layer_name):
    """
    Performs basic data validation checks for a given DataFrame.
    Args:
        df: Spark DataFrame.
        layer_name: Name of the data layer (e.g., 'Bronze', 'Silver', 'Gold - Dim Customer').
    Returns:
        True if all checks pass, False otherwise.
    """
    print(f"\n--- Data Validation for {layer_name} ---")
    valid = True

    # Example: Check for empty DataFrame
    if df.count() == 0:
        print(f"WARNING: {layer_name} DataFrame is empty!")
        valid = False
    else:
        print(f"INFO: {layer_name} DataFrame has {df.count()} rows.")

    # Example: Check for nulls in critical columns (customize per layer)
    critical_columns = []
    if layer_name == "Silver":
        critical_columns = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]
    elif "Gold - Dim Customer" in layer_name:
        critical_columns = ["customerID"]
    elif "Gold - Fact Churn" in layer_name:
        critical_columns = ["customerID", "Contract", "InternetService", "PaymentMethod"]
    
    for col_name in critical_columns:
        null_count = df.filter(df[col_name].isNull()).count()
        if null_count > 0:
            print(f"WARNING: Column '{col_name}' in {layer_name} has {null_count} null values.")
            valid = False
        else:
            print(f"INFO: Column '{col_name}' in {layer_name} has no null values.")

    # Add more specific validation rules as needed (e.g., value ranges, uniqueness for PKs)

    if valid:
        print(f"--- {layer_name} Validation PASSED ---")
    else:
        print(f"--- {layer_name} Validation FAILED ---")
    
    return valid

def user_approval_needed():
    """
    Prompts the user for approval to continue.
    Returns:
        True if user approves, False otherwise.
    """
    response = input("Do you want to continue to the next stage? (yes/no): ").lower().strip()
    return response == 'yes'