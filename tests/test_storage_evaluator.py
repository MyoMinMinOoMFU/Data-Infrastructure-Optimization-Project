from pathlib import Path

import yaml

from storage_evaluator import evaluate_storage, load_infrastructure_config

CONFIG_FILE = Path("Infrastructure_baseline.yaml")


def test_load_infrastructure_config():
	config = load_infrastructure_config(CONFIG_FILE)

	assert "infrastructure" in config
	assert "storage" in config["infrastructure"]
	assert "security" in config["infrastructure"]


def test_evaluate_storage_passes(capsys):
	config = load_infrastructure_config(CONFIG_FILE)

	evaluate_storage(config)

	output = capsys.readouterr().out

	assert "=== Storage Evaluation ===" in output
	assert "PASS: Encryption is enabled." in output
	assert "PASS: Public access is disabled." in output
	assert "PASS: IAM authentication is configured." in output
	assert "PASS: Least-privilege authorization is configured." in output
	assert "PASS: Lifecycle policy is enabled." in output
	assert "PASS: Network isolation is enabled." in output
	assert "PASS: Private subnets are enabled." in output
	assert "PASS: Security groups are enabled." in output
	assert "PASS: TLS encryption is configured for data in transit." in output


def test_evaluate_storage_detects_disabled_encryption(capsys):
	config = load_infrastructure_config(CONFIG_FILE)

	config["infrastructure"]["storage"]["data_lake"][
		"bucket_config"
	]["encryption"]["enabled"] = False

	evaluate_storage(config)

	output = capsys.readouterr().out

	assert "FAIL: Encryption is disabled." in output


def test_evaluate_storage_detects_public_access(capsys):
	config = load_infrastructure_config(CONFIG_FILE)

	config["infrastructure"]["storage"]["data_lake"][
		"bucket_config"
	]["access_control"]["public_access"] = True

	evaluate_storage(config)

	output = capsys.readouterr().out

	assert "FAIL: Public access is enabled." in output


def test_evaluate_storage_detects_non_iam_authentication(capsys):
	config = load_infrastructure_config(CONFIG_FILE)

	config["infrastructure"]["storage"]["data_lake"][
		"bucket_config"
	]["access_control"]["authentication"] = "PASSWORD"

	evaluate_storage(config)

	output = capsys.readouterr().out

	assert "WARNING: IAM authentication is not configured." in output
