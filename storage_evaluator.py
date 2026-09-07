import yaml


def load_infrastructure_config(file_path):
    """Load infrastructure configuration from YAML."""
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def evaluate_storage(config):
    """Evaluate data lake storage configuration."""
    storage = config["infrastructure"]["storage"]["data_lake"]
    bucket = storage["bucket_config"]

    results = []

    # Encryption check
    if bucket["encryption"]["enabled"]:
        results.append("PASS: Encryption is enabled.")
    else:
        results.append("FAIL: Encryption is disabled.")

    # Public access check
    if not bucket["access_control"]["public_access"]:
        results.append("PASS: Public access is disabled.")
    else:
        results.append("FAIL: Public access is enabled.")

    # Authentication check
    if bucket["access_control"]["authentication"] == "IAM":
        results.append("PASS: IAM authentication is configured.")
    else:
        results.append("WARNING: IAM authentication is not configured.")

    # Authorization check
    if bucket["access_control"]["authorization"] == "least_privilege":
        results.append("PASS: Least-privilege authorization is configured.")
    else:
        results.append(
            "WARNING: Least-privilege authorization is not configured."
        )

    # Lifecycle check
    if bucket["lifecycle_policy"]["enabled"]:
        results.append("PASS: Lifecycle policy is enabled.")
    else:
        results.append("WARNING: Lifecycle policy is disabled.")

    # Network isolation check
    security = config["infrastructure"]["security"]

    if security["network_isolation"]["enabled"]:
        results.append("PASS: Network isolation is enabled.")
    else:
        results.append("FAIL: Network isolation is disabled.")

    # Private subnet check
    if security["network_isolation"]["private_subnets"]:
        results.append("PASS: Private subnets are enabled.")
    else:
        results.append("FAIL: Private subnets are disabled.")

    # Security groups check
    if security["network_isolation"]["security_groups_enabled"]:
        results.append("PASS: Security groups are enabled.")
    else:
        results.append("FAIL: Security groups are disabled.")

    # TLS check
    if security["encryption"]["data_in_transit"] == "TLS":
        results.append("PASS: TLS encryption is configured for data in transit.")
    else:
        results.append("WARNING: TLS encryption is not configured.")

    print("=== Storage Evaluation ===")
    print(f"Bucket name: {bucket['bucket_name']}")
    print()

    for result in results:
        print(result)


if __name__ == "__main__":
    config = load_infrastructure_config(
        "Infrastructure_baseline.yaml"
    )

    evaluate_storage(config)