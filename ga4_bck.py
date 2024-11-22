import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy, Filter, \
    FilterExpression
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account


class GA4Report():
    def __init__(self, dealer_name: str, credentials: Credentials):
        self.dealer_name = dealer_name
        self.client = BetaAnalyticsDataClient(credentials=credentials)
        self.limit = 10_000

    def run_report(self) -> pd.DataFrame:
        data = []
        offset = 0
        while True:
            request = RunReportRequest(
                property=f"properties/302263505",
                dimensions=[3, Dimension(name="date")],
                metrics=[Metric(name="screenPageViews")],
                order_bys=[OrderBy(dimension={'dimension_name': 'pagePath'})],
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



dealer_name = "hooftman.nl"
key_file_location = f"json_{dealer_name}.json"
credentials = service_account.Credentials.from_service_account_file(
    str(key_file_location), scopes=['https://www.googleapis.com/auth/analytics.readonly'])
report = GA4Report(dealer_name, credentials)
result = report.run_report()
result.to_csv(f"{dealer_name}.csv", index=False)

#dit bestand uitvoeren door python ga4.py