# cdc_eic_daily_news
This is a repository for the manscript's abstract
# Evaluation of the Performance of Taiwan’s International Epidemic Intelligence Mechanism (2009–2025)
Hao-Kai Tseng, Wei-Ju Tseng, Tzu-Chuan Huang, Wei-Che Sun, Ching-Wen Chang, Chun-Wei Tung, Hsien-Chun Chiu, Ting-Yu Zheng, Chien-Pang Clark Hsu, Chiu-Mei Chen, Chia-Lin Lee, Yu-Lun Liu, Hung-Wei Kuo

Abstract
Taiwan’s international epidemic intelligence mechanism is designed for the real-time monitoring and public notification of global infectious disease threats of significant concern. We evaluate the mechanism’s performance from 2009 to 2025 using open data from the Taiwan Centers for Disease Control (TCDC), focusing on three thematic metrics: importance, timeliness, and information impact.

Based on 73,344 country-disease incidents in the International Epidemic News (IEN), the mechanism functioned as a responsive intelligence framework where surveillance priorities evolved in alignment with domestic and global trends. The most frequently monitored diseases - novel influenza A, COVID-19, dengue fever, poliomyelitis, and measles - closely mirrored the priorities featured in the World Health Organization (WHO) Disease Outbreak News. 

Reflecting a risk-based approach, surveillance was concentrated on Taiwan’s high-volume travel routes, such as the United States, China, Hong Kong, Japan, and the United Kingdom. Negative binomial regression showed that during WHO-declared Public Health Emergencies of International Concern (PHEIC) periods, IEN's weekly relative reporting rate for the declared disease increased 9.28-fold (95% CI: 3.48-24.79) compared to pre-declaration levels.

Regarding timeliness, the IEN exhibited high operational stability, with an average publication lag of only 3 days from original news sources. Reflecting high public engagement, the International Travel Health Notice became TCDC’s most-downloaded dataset on the Open Data portal, totaling 67,305 downloads. Artificial Intelligence (AI) analysis using the Gemini 2.5 Flash model found that 42% of TCDC news releases contained international intelligence in average, underscoring its central role in risk communication.

In conclusion, the mechanism effectively fulfilled its mandate by operating with high precision, timeliness, and impact. To bolster national resilience against emerging and re-emerging health threats, future efforts should prioritize deeper AI integration to enhance early warning capabilities and continue optimizing risk communication.


- Key words: International epidemics intelligence, International Travel Health Notices, Public Health Emergency of International Concern, monitoring and evaluation, artificial intelligence 

- - - - 
# Prompt Engineering
The following System Instruction was used with the Gemini 2.5 Flash model to classify whether Taiwan CDC news releases contain international epidemic intelligence.

```
SYSTEM_INSTRUCTION = """
# 角色
你是一位臺灣疾病管制署的流行病學家。
# 任務
二分法分類任務為判斷新聞稿是否提及「臺灣以外的國際疫情資訊」，準則如下：
## 判定為'1'(是)的準則(任一項即符合)
- 文中提及國外(如中國、東南亞區域、全球等）的病例數、規模、流行型別、或疫情趨勢(範例：香港累計報告10例病例、歐洲疫情下降、全球疫情嚴峻、WHO指出目前BA.5及其衍生變異株仍為全球主流株)。
- 文中提及「國際旅遊疫情建議等級」(範例：全球疫情等級第二級、日本疫情等級第一級)。
## 判定為'0'(否)的準則(任一項即符合)
- 僅提到地名疾病(如：日本腦炎、德國麻疹），但個案在臺灣且未描述外國疫情資訊(範例：國內新增5例日本腦炎確定病例)。
- 僅提及臺灣的「境外移入個案」，但並未描述該來源國的疫情資訊(範例：國內境外移入之2例個案，分別自中國及越南移入)。
- 其餘任何不完全符合「判定為 1(是)」準則之內容，一律判定為'0'。
## 任務執行要求
請評估新聞稿內容，並嚴格以JSON格式回復如下： 
- analysis：分析並簡述文中是否包含「臺灣以外的國際疫情資訊」。
- result：僅回覆'1'或'0'。
## 輸出範例
{"analysis":"文中提到泰國病例數，符合準則1。", "result":1}
"""
```
For the complete implementation and data pipeline, please refer to [main.ipynb](https://github.com/haokaitseng/cdc_eic_daily_news/blob/main/main.ipynb).