# coding=utf-8

import pandas as pd
import pytest

from pyscenic.prune import prune2df

def test_prune2df_generator_bug():
    from dask.distributed import Client, LocalCluster
    from ctxcore.genesig import Regulon
    from unittest.mock import patch, MagicMock

    cluster = LocalCluster(n_workers=1, threads_per_worker=1)
    custom_client = Client(cluster)

    def dummy_modules2df(*args, **kwargs):
        from pyscenic.transform import DF_META_DATA
        return DF_META_DATA.copy()

    def fake_from_delayed(dfs, meta=None):
        import dask.dataframe as dd
        return dd.from_delayed(dfs, meta=meta)

    with patch("pyscenic.prune.load_motif_annotations") as mock_load, \
         patch("pyscenic.prune.from_delayed", new=fake_from_delayed), \
         patch("pyscenic.prune.modules2df", new=dummy_modules2df):

        mock_load.return_value = pd.DataFrame()

        class DummyDB:
            name = "db1"
        db = DummyDB()
        rnkdbs = [db]

        regulon = Regulon(name="reg1", gene2weight={"G1": 1.0}, gene2occurrence={}, transcription_factor="TF1")
        modules = [regulon]

        motif_annotations_fname = "dummy.csv"

        # If Dask cannot handle the generator expression, it raises a TypeError
        # We test that it completes without throwing this type error
        res = prune2df(
            rnkdbs=rnkdbs,
            modules=modules,
            motif_annotations_fname=motif_annotations_fname,
            client_or_address=custom_client
        )
