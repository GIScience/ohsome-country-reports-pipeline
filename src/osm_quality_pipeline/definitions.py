from pathlib import Path
import dagster as dg


@dg.definitions
def defs():
    loaded_defs = dg.load_from_defs_folder(
        project_root=Path(__file__).parent.parent.parent
    )

    return dg.Definitions.merge(
        loaded_defs,
        dg.Definitions(
            executor=dg.multiprocess_executor.configured(
                {
                    "max_concurrent": 5,
                }
            ),
        ),
    )
