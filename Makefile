.PHONY: export-artifacts bi-up configure bootstrap up demo smoke test integration dbt ops-up ops-down reconcile export-tableau archive status down reset-demo pause resume scenario drain restart-processing
export-artifacts bi-up configure bootstrap up demo smoke test integration dbt ops-up ops-down reconcile archive status down reset-demo pause resume drain restart-processing:
	python3 scripts/cloud.py $@
export-tableau:
	python3 scripts/cloud.py export
scenario:
	python3 scripts/cloud.py scenario $(SCENARIO)
