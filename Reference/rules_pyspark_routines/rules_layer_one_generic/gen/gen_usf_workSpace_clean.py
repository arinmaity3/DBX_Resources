# Databricks notebook source
def gen_usf_workSpace_clean(errorDescription = ""):
    try:
        if (errorDescription is not None and errorDescription.strip() != ""):
            gen_usf_cachedObjects_clean()
    except Exception as e:
        raise
