from wheely.mammoth.proteins import (
    ProteinConfidenceDataset as _ProteinConfidenceDataset,
    ProteinIntensityConfidenceDataset as _ProteinIntensityConfidenceDataset,
    ProteinIntensityDataset as _ProteinIntensityDataset,
)


def merge_protein_confidence_and_quant(
    prot_conf: _ProteinConfidenceDataset,
    prot_quant_dset: _ProteinIntensityDataset,
) -> _ProteinIntensityConfidenceDataset:
    """
    Merge a datasaet with global protein confidence estimates with a dataset containg per-sample protein quantifications.

    IMPORTANT: this will produce duplicated rows if multiple confidence estimates exist for the same protein.

    Parameters
    ----------
    prot_conf : ProteinConfidenceDataset
        The protein confidence dataset
    prot_quant_dset : ProteinIntensityDataset
        The protein quantification dataset

    Returns
    -------
    A combined ``ProteinIntensityConfidenceDataset``
    """
    intensity_columns = list(prot_quant_dset.intensity_columns)

    return _ProteinIntensityConfidenceDataset(
        prot_conf.data.join(
            prot_quant_dset.data.select(
                prot_quant_dset.proteins.alias(prot_conf.protein_column),
                prot_quant_dset.samples,
                *intensity_columns,
            ),
            on=prot_conf.protein_column,
            how="leftouter",
        ),
        sample_column=prot_quant_dset.sample_column,
        intensity_column=prot_quant_dset.intensity_column,
        intensity_columns=intensity_columns,
        protein_column=prot_conf.protein_column,
        protein_delim=prot_conf.protein_column,
        target_column=prot_conf.target_column,
        score_columns=prot_conf.score_columns,
        qvalue_column=prot_conf.qvalue_column,
        errprob_column=prot_conf.errprob_column,
        pi0=prot_conf.pi0,
        semantics=dict(
            **{
                k: v
                for k in [
                    prot_quant_dset.sample_column,
                    *intensity_columns,
                ]
                if (v := prot_quant_dset.semantics.get(k, None)) is not None
            },
            **prot_conf.semantics,
        ),
    )
