import logging
from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd

class TushareDataTransformer:
    def __init__(self, logger):
        self.logger = logger

    def validate_data(self, data):
        original_count = len(data)
        filtered_data = data.dropna()
        filtered_count = len(filtered_data)

        if filtered_count < original_count:
            self.logger.warning(
                f"数据验证: 过滤掉 {original_count - filtered_count} 行不符合规则的数据"
            )

        return cast(pd.DataFrame, filtered_data) 