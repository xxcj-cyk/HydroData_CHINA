"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-04-26 11:12:10
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-04-27 11:02:27
"""

from hydrodata_china.datasets.sanxia import Sanxia_1D

# 测试函数
if __name__ == "__main__":
    # 初始化类
    sanxia_1d = Sanxia_1D()
    sanxia_1d.cache_attributes_xrdataset()
    sanxia_1d.cache_forcing_xrdataset()
    sanxia_1d.cache_target_xrdataset()
    sanxia_1d.cache_xrdataset()