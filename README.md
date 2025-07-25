This is a basic demonstration of the Gemini-2.0-flash LLM to gemerate SQL queries to be run on big data to extract meaningful insights from it.

Script.py converts the dataset into Parquet files (columnar storage) for easy and fast readability.
Bot.py imports the Gemini free-tier API and describes a prompt to it based on which it generates SQL queries to run and get meaningful insights.

The data used for this demonstration is purely SYNTHETIC and does not represent any actual Bank data.

The output from Bot.py was cross validated by creating pivot tables on google sheets.

Images are attached.

PERCENTAGE OF FINTECH AND NON-FINTECH

<img width="492" height="159" alt="fintech_perc_pivot" src="https://github.com/user-attachments/assets/d895a61d-fe62-49bc-8920-e5feed5a4a75" />
<img width="873" height="233" alt="fintech_perc_bot" src="https://github.com/user-attachments/assets/2ae21f0b-6222-4c1e-902b-99789251a738" />

PERCENTAGE OF SANCTION AMOUNT FOR KARNATAKA

<img width="474" height="112" alt="perc_state_pivot" src="https://github.com/user-attachments/assets/c32f96b8-d183-4bde-8b74-9dce2b0628eb" />
<img width="1016" height="195" alt="perc_state_bot" src="https://github.com/user-attachments/assets/6672db19-bd9c-4bf0-ae21-09c432771db1" />

SUM OF SANCTION AMOUNT FOR FINTECH/NON-FINTECH

<img width="461" height="168" alt="sanc_fintech_pivot" src="https://github.com/user-attachments/assets/6b407acc-562d-441d-b9ec-f3db044a629a" />
<img width="535" height="252" alt="sanc_fintech_bot" src="https://github.com/user-attachments/assets/4319e966-9b50-450c-9a37-50f738caf49e" />

TOP TEN STATES BASED ON SANCTION AMOUNT

<img width="585" height="281" alt="Top_10_sanc_pivot" src="https://github.com/user-attachments/assets/928e839f-4cc3-4afc-9cf5-9a20c2444c3c" />
<img width="674" height="415" alt="Top_10_sanc_bot" src="https://github.com/user-attachments/assets/b6292851-a678-4d0f-bb94-cf2fd536290e" />


these are a few examples. the better we make the schema prompt, the more complex questions we can ask from the bot, such as time series analysis and averages.

DOES THIS IMPACT MY DATA SECURITY??

NO. none of this data is actually passed to Google. The LLMs task is ONLY to generate SQL queries. The LLM knows ONLY the schema of the database, and nothing from the data itself. SO, this is 100% safe.
SQL query execution is done locally.
