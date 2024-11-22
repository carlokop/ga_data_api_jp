import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy, Filter, \
    FilterExpression
# from google.oauth2.service_account import Credentials
from google.oauth2 import service_account


class GA4Report():
    #Fetched data from Google Analytics API

    def __init__(self, dealer_name: str, property: str):
        #Inits Google Analytics API
        key_file_location = f"json/json_{dealer_name}.json"
        credentials = service_account.Credentials.from_service_account_file(
            str(key_file_location), scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )

        self.property = property
        self.dealer_name = dealer_name
        self.client = BetaAnalyticsDataClient(credentials=credentials)
        self.limit = 10_000

    def run_report_session(self) -> pd.DataFrame:
        """
        Fetches data from API and creates a report with session_source/session_medium combined with page_path and date
        """
        data = []
        offset = 0
        while True:
            request = RunReportRequest(
                property=f"properties/" + self.property,
                dimensions=[Dimension(name="sessionSourceMedium"), Dimension(name="pagePath"), Dimension(name="date")],
                metrics=[Metric(name="screenPageViews")],
                order_bys=[
                    OrderBy(dimension={'dimension_name': 'pagePath'}),
                    OrderBy(dimension={'dimension_name': 'date'}),
                    OrderBy(dimension={'dimension_name': 'sessionSourceMedium'})
                ],
                date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
                dimension_filter=FilterExpression(
                    filter=Filter(
                        field_name="pagePath",
                        string_filter=Filter.StringFilter(
                            match_type="PARTIAL_REGEXP", value='.+'
                        )
                    )
                ),
                limit=self.limit,
                offset=offset
            )
            response = self.client.run_report(request)
            for row in response.rows:
                data.append(
                    [dimension_value.value for dimension_value in row.dimension_values] +
                    [metric_value.value for metric_value in row.metric_values]
                )
            offset += self.limit
            if offset > response.row_count:
                break
        return pd.DataFrame(data=data)

    def run_report_conversions(self) -> pd.DataFrame:
        """
        Fetches data from API and creates a report with event and conversion data
        """
        data = []
        offset = 0
        while True:
            request = RunReportRequest(
                property=f"properties/" + self.property,
                dimensions=[
                    # Dimension(name="sessionSourceMedium"), #session source/medium
                    Dimension(name="eventName"),
                    Dimension(name="isKeyEvent"),   #true is a conversion event
                    Dimension(name="pagePath"),     #event took place on this url
                    Dimension(name="date")
                ],
                metrics=[Metric(name="eventCount")],
                order_bys=[
                    OrderBy(dimension={'dimension_name': 'eventName'}),
                    OrderBy(dimension={'dimension_name': 'pagePath'}),
                    OrderBy(dimension={'dimension_name': 'date'})
                ],
                date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
                dimension_filter=FilterExpression(
                    filter=Filter(
                        field_name="pagePath",
                        string_filter=Filter.StringFilter(
                            match_type="PARTIAL_REGEXP", value='.+'
                        )
                    )
                ),
                limit=self.limit,
                offset=offset
            )
            response = self.client.run_report(request)
            for row in response.rows:
                data.append(
                    [dimension_value.value for dimension_value in row.dimension_values] +
                    [metric_value.value for metric_value in row.metric_values]
                )
            offset += self.limit
            if offset > response.row_count:
                break
        return pd.DataFrame(data=data)


# alleen dit aanpassen
property = 302263505
dealer_name = "hooftman.nl"

# run report
report = GA4Report(dealer_name, str(property))
result = report.run_report_session()
# result = report.run_report_conversions()
result.to_csv(f"csv/{dealer_name}.csv", index=False)

#python ga4-session.py

