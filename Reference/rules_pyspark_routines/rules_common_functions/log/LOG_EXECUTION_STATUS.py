# Databricks notebook source
from enum import Enum

class LOG_EXECUTION_STATUS(Enum):    
    FAILED = 0
    SUCCESS = 1
    IN_PROGRESS = 2
    STARTED = 3
    WARNING = 4
    NOT_STARTED = 5
    NOT_APPLICABLE = 7
    COMPLETED = 8

class LOG_VALIDATION_RESULT_FILE(Enum):
    SUMMARY_FILE = 'ValidationSummary-'
    DETAIL_FILE = 'ValidationDetails-'
