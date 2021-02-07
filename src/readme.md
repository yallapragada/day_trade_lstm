

<h3>iex_pre_processing_v2_stg1.py</h3>
takes a iex data file and generates an output file with following fields:
ticker,date,price_2,price_4,price_6,price_8,price_10,price_12,price_14,price_16,price_18,price_20,p_sma1,p_sma2,p_sma3,v_sma1,v_sma2,v_sma3,p_ema1,p_ema2,p_ema3,v_ema1,v_ema2,v_ema3,target

<h3>iex_pre_processing_v2_stg2.py</h3>
takes output from iex_pre_processing_v2_stg1.py and generates an output file with following fields:
price_diff_2_4,price_diff_4_6,price_diff_6_8,price_diff_8_10,price_diff_10_12,price_diff_12_14,price_diff_14_16,price_diff_16_18,price_diff_18_20,p_sma_12,p_sma_13,p_sma_23,p_ema_12,p_ema_13,p_ema_23,v_sma_12,v_sma_13,v_sma_23,v_ema_12,v_ema_13,v_ema_23,target

<h3>iex_pre_processing_v2_stg2_01.py</h3>
takes output from iex_pre_processing_v2_stg1.py and generates a binary data file with following fields:
price_diff_2_4,price_diff_4_6,price_diff_6_8,price_diff_8_10,price_diff_10_12,price_diff_12_14,price_diff_14_16,price_diff_16_18,price_diff_18_20,p_sma_12,p_sma_13,p_sma_23,p_ema_12,p_ema_13,p_ema_23,v_sma_12,v_sma_13,v_sma_23,v_ema_12,v_ema_13,v_ema_23,target

<h3>iex_pre_processing_v2_stg21.py</h3>
takes output from iex_pre_processing_v2_stg1.py and generates an output file with following fields:
uses marketClose instead of marketAverage
price_diff_2_4,price_diff_4_6,price_diff_6_8,price_diff_8_10,price_diff_10_12,price_diff_12_14,price_diff_14_16,price_diff_16_18,price_diff_18_20,p_sma_12,p_sma_13,p_sma_23,p_ema_12,p_ema_13,p_ema_23,v_sma_12,v_sma_13,v_sma_23,v_ema_12,v_ema_13,v_ema_23,target
