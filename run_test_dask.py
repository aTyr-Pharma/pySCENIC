import os
from dask.distributed import Client, LocalCluster
from pyscenic.prune import prune2df
from ctxcore.genesig import Regulon
import pandas as pd
from unittest.mock import patch, MagicMock

def test_prune2df_generator_bug():
    print("Starting cluster...")
    cluster = LocalCluster(n_workers=1, threads_per_worker=1)
    custom_client = Client(cluster)
    
    # We will mock load_motif_annotations, module2features_auc1st_impl, from_delayed
    with patch("pyscenic.prune.load_motif_annotations") as mock_load, \
         patch("pyscenic.prune.from_delayed") as mock_from_delayed, \
         patch("pyscenic.prune.modules2df") as mock_mod2df:
         
        mock_load.return_value = pd.DataFrame()
        # mock from_delayed to just return what it is passed, so we can see the TypeError
        def fake_from_delayed(dfs, meta=None):
            import dask.dataframe as dd
            # we call the real from_delayed to reproduce the error
            return dd.from_delayed(dfs, meta=meta)
            
        mock_from_delayed.side_effect = fake_from_delayed
        
        db = MagicMock()
        db.name = "db1"
        rnkdbs = [db]
        
        regulon = Regulon(name="reg1", gene2weight={"G1": 1.0}, gene2occurrence={}, transcription_factor="TF1")
        modules = [regulon]
        
        motif_annotations_fname = "dummy.csv"
        
        try:
            prune2df(
                rnkdbs=rnkdbs,
                modules=modules,
                motif_annotations_fname=motif_annotations_fname,
                client_or_address=custom_client
            )
            print("SUCCESS")
        except TypeError as e:
            print("CAUGHT TYPE ERROR:")
            print(e)
        except Exception as e:
            print("CAUGHT OTHER ERROR:")
            print(e)
            
if __name__ == "__main__":
    test_prune2df_generator_bug()
