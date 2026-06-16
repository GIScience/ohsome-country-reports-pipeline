import dagster as dg

ALL_COUNTRIES = ["RWA", "SYR", "STP"]
country_partitions = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)