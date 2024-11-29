import json


class PrometheusApiData:

    @staticmethod
    def get_prometheus_webpos_url(workspace_id:str, region:str):

        endpoint = f'https://aps-workspaces.{region}.amazonaws.com/workspaces/{workspace_id}/api/v1/query_range'
        return endpoint


    @staticmethod
    def get_prometheus_webpos_params(set_date_start, set_date_end):

        if isinstance(set_date_start, str):
            date_start = set_date_start
        else:
            date_start = set_date_start.strftime('%Y-%m-%d')
        if isinstance(set_date_end, str):
            date_end = set_date_end
        else:
            date_end = set_date_end.strftime('%Y-%m-%d')
        interval = '5m'
        params = {
            'query': f'histogram_quantile(0.95, sum(rate(vpos_response_time_distribution_bucket{{tasktype="request"}}[{interval}])) by (le,tasktype))',
            'start': f'{date_start}T00:00:00Z',
            'end': f'{date_end}T23:59:59Z',
            'step': interval
        }

        return params
