import json
import json
from datetime import datetime, date
from utils import get_period_range

class ApiData:

    @staticmethod
    def get_checkout_url():
        url = 'https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-sla-cash-by-ufilial/_search'
        return url

    @staticmethod
    def get_sco_url():
        url = 'https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-sla-sco-by-ufilial/_search'
        return url

    @staticmethod
    def get_scales_url():
        url = 'https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-sla-scales-by-ufilial/_search'
        return url

    @staticmethod
    def get_url_scales_by_ufilial():
        url = "https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-scales-by-ufilial/_search"
        return url

    @staticmethod
    def get_url_cashcso_by_ufilial():
        url = "https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-cashcso-by-ufilial/_search"
        return url

    @staticmethod
    def get_url_pos_sd_needcashdesks(level: str):
        if level == "ufilial":
            url = "https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-needcashdesks-by-ufilial/_search"
        else:
            print(f"method get_url_pos_sd_needcashdesks, incorrect parameters: level = {level}. \nlevel = ['ufilial']")
            exit(1)
        return url

    @staticmethod
    def get_url_pos_sd_sla_sco(level: str = None):
        """level: 'ufilial'"""
        if level == "ufilial":
            url = 'https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-sla-sco-by-ufilial/_search'
        else:
            print(f"method get_url_pos_sd_sla_sco, incorrect parameters: level = {level}. \nlevel = ['ufilial','business']")
            exit(1)
        return url

    @staticmethod
    def get_url_plan_by_ufilial(type:str, level: str):
        if level == "ufilial":
            if type == "cash":
                url_part = "pos-mle-cash-by-ufilial"
            elif type == "sco":
                url_part = "pos-mle-sco-by-ufilial"
            elif type == "scales":
                url_part = "pos-mle-scales-by-ufilial"
            else:
                print(f"method get_url_plan_by_ufilial, incorrect parameters: type = {type}, level = {level}. \n Correct value type = ['cash', 'sco','scales'] level = ['ufilial','business']")
                exit(1)
        else:
            print(f"method get_url_plan_by_ufilial, incorrect parameters: type = {type}, level = {level}. \n Correct value type = ['cash', 'sco','scales'] level = ['ufilial','business']")
            exit(1)
        url = f"https://sfpv-elas001.fz.fozzy.lan:9200/{url_part}/_search"
        return url

    @staticmethod
    def get_url_signature_servers():
        url = 'https://sfpv-elas001.fz.fozzy.lan:9200/sla-gryada-test*/_search'
        return url

    @staticmethod
    def get_url_devices_total_by_filial():
        url = 'https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-total-by-ufilial/_search'
        return url

    @staticmethod
    def get_scales_paper_state_url():
        url = "https://sfpv-elas001.fz.fozzy.lan:9200/pos-sd-sla-scales-hw-by-ufilial/_search"
        return url

    @staticmethod
    def get_objects_url(token:str, start:date, end:date) -> str:
        url = f"https://nmcs.fozzy.lan/api/get-object-availability/dtc/it-dashboard/?token={token}&datemin={start}T00:00:00&datemax={end}T00:00:00"
        return url

    @staticmethod
    def get_object_period_dates(current_date, period_type):
        if current_date.day == 1 and current_date.month != 1:
            if current_date.month in [4, 7, 10] and period_type == "quarter":
                date_start, date_end = get_period_range(period_type, current_date)
            elif period_type == "month":
                date_start, date_end = get_period_range(period_type, current_date)
            else:
                return None, None

        elif current_date.day == 10 and current_date.month == 1 and period_type in ["quarter", "month", "year"]:
            date_start, date_end = get_period_range(period_type, current_date)
        else:
            return None, None

        print(f"\nGetting url for {period_type} with date_start: {date_start} and date_end: {date_end}")
        return date_start, date_end

    @staticmethod
    def get_checkout_body(set_date_start, set_date_end, interval: str = '30m'):

        if isinstance(set_date_start, str):
            date_start = set_date_start
        else:
            date_start = set_date_start.strftime("%Y-%m-%d")
        if isinstance(set_date_end, str):
            date_end = set_date_end
        else:
            date_end = set_date_end.strftime("%Y-%m-%d")
        body = json.dumps({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": f"{date_start}T00:00:00",
                                    "lt": f"{date_end}T00:00:00",
                                    "time_zone": "Europe/Kiev"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "by_ufilial": {
                    "terms": {
                        "field": "UFILIAL",
                        "size": 10000
                    },
                    "aggs": {
                        "time_histogram": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": f"{interval}",
                                "time_zone": "Europe/Kiev"
                            },
                            "aggs": {
                                "ok": {
                                    "avg": {
                                        "field": "ok"
                                    }
                                },
                                "planned": {
                                    "avg": {
                                        "field": "needed"
                                    }
                                },
                                "tags": {
                                    "top_hits": {
                                        "size": 1,
                                        "_source": {
                                            "includes": [
                                                "within_working_hours",
                                                "md.marketstatusid",
                                                "md.marketstatusname",
                                                "md.cashdeskcount",
                                                "md.businessid"
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        return body

    @staticmethod
    def get_sco_body(set_date_start, set_date_end, interval: str = '30m'):

        if isinstance(set_date_start, str):
            date_start = set_date_start
        else:
            date_start = set_date_start.strftime("%Y-%m-%d")
        if isinstance(set_date_end, str):
            date_end = set_date_end
        else:
            date_end = set_date_end.strftime("%Y-%m-%d")
        body = json.dumps({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": f"{date_start}T00:00:00",
                                    "lt": f"{date_end}T00:00:00",
                                    "time_zone": "Europe/Kiev"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "by_ufilial": {
                    "terms": {
                        "field": "UFILIAL",
                        "size": 10000
                    },
                    "aggs": {
                        "time_histogram": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": f"{interval}",
                                "time_zone": "Europe/Kiev"
                            },
                            "aggs": {
                                "ok": {
                                    "avg": {
                                        "field": "ok"
                                    }
                                },
                                "planned": {
                                    "avg": {
                                        "field": "md.scocount"
                                    }
                                },
                                "ml_median": {
                                    "avg": {
                                        "field": "ml_median"
                                    }
                                },
                                "inRepair": {
                                    "max": {
                                        "field": "md.inrepair"
                                    }
                                },
                                "tags": {
                                    "top_hits": {
                                        "size": 1,
                                        "_source": {
                                            "includes": [
                                                "within_working_hours",
                                                "md.marketstatusid",
                                                "md.marketstatusname",
                                                "md.cashdeskcount",
                                                "md.businessid"
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        return body

    @staticmethod
    def get_scales_body(set_date_start, set_date_end, interval: str = '30m'):

        if isinstance(set_date_start, str):
            date_start = set_date_start
        else:
            date_start = set_date_start.strftime("%Y-%m-%d")
        if isinstance(set_date_end, str):
            date_end = set_date_end
        else:
            date_end = set_date_end.strftime("%Y-%m-%d")
        body = json.dumps({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": f"{date_start}T00:00:00",
                                    "lt": f"{date_end}T00:00:00",
                                    "time_zone": "Europe/Kiev"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "by_ufilial": {
                    "terms": {
                        "field": "UFILIAL",
                        "size": 10000
                    },
                    "aggs": {
                        "time_histogram": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": f"{interval}",
                                "time_zone": "Europe/Kiev"
                            },
                            "aggs": {
                                "ok": {
                                    "avg": {
                                        "field": "ok"
                                    }
                                },
                                "ml_median": {
                                    "avg": {
                                        "field": "ml_median"
                                    }
                                },
                                "tags": {
                                    "top_hits": {
                                        "size": 1,
                                        "_source": {
                                            "includes": [
                                                "within_working_hours",
                                                "md.marketstatusid",
                                                "md.marketstatusname",
                                                "md.cashdeskcount",
                                                "md.businessid"
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        return body


    @staticmethod
    def get_body_scales_by_ufilial(date_today, date_set, interval: str = '30m'):
        if isinstance(date_set, str):
            date = date_set
        else:
            date = date_set.strftime("%Y-%m-%d")
        today = date_today.strftime("%Y-%m-%d")
        body = json.dumps({
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [
                                    {
                                    "range": {
                                        "timestamp": {
                                            "format": "strict_date_optional_time",
                                            "gte": f"{date}T00:00:00",
                                            "lt": f"{today}T00:00:00",
                                            "time_zone": "Europe/Kiev"
                                                }
                                             }
                                    }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    },
                    "aggs": {
                        "UFILIAL": {
                            "terms": {
                                "field": "UFILIAL",
                                "size": 10000
                            },
                            "aggs": {
                                "time_histogram": {
                                    "date_histogram": {
                                        "field": "timestamp",
                                        "fixed_interval": f"{interval}",
                                        "time_zone": "Europe/Kiev"
                                    },
                                    "aggs": {
                                        "OK": {
                                            "avg": {
                                                "field": "OK_state.unique_hosts"
                                            }
                                        },
                                        "NOT_OK": {
                                            "avg": {
                                                "field": "NOT_OK_state.unique_hosts"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )
        return body

    @staticmethod
    def get_body_cashcso_by_ufilial(date_today, date_set, valu_type: str, interval: str = '30m'):
        if isinstance(date_set, str):
            date = date_set
        else:
            date = date_set.strftime("%Y-%m-%d")
        today = date_today.strftime("%Y-%m-%d")
        body = json.dumps({
                    "size" : 0,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "devtype": {
                                            "value": valu_type
                                        }
                                    }
                                }
                            ],
                            "filter": [
                                {
                                    "range": {
                                        "timestamp": {
                                            "format": "strict_date_optional_time",
                                            "gte": f"{date}T00:00:00",
                                            "lt": f"{today}T00:00:00",
                                            "time_zone": "Europe/Kiev"
                                        }
                                    }
                                }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    },
                    "aggs": {
                        "by_ufilial": {
                            "terms": {
                                "field": "UFILIAL",
                                "size": 10000
                            },
                            "aggs": {
                                "time_histogram": {
                                    "date_histogram": {
                                        "field": "timestamp",
                                        "fixed_interval": f"{interval}",
                                        "time_zone": "Europe/Kiev"
                                    },
                                    "aggs": {
                                        "OK_state_avg": {
                                            "avg": {
                                                "field": "OK_state.unique_hosts"
                                            }
                                        },
                                        "Warning_state_avg": {
                                            "avg": {
                                                "field": "Warning_state.unique_hosts"
                                            }
                                        },
                                        "Critical_state_avg": {
                                            "avg": {
                                                "field": "Critical_state.unique_hosts"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )
        return body


    @staticmethod
    def get_body_pnal_by_ufilial(date_today, date_set, interval: str = '30m'):
        if isinstance(date_set, str):
            date = date_set
        else:
            date = date_set.strftime("%Y-%m-%d")
        today = date_today.strftime("%Y-%m-%d")
        body = json.dumps({
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "timestamp": {
                                            "format": "strict_date_optional_time",
                                            "gte": f"{date}T00:00:00",
                                            "lt": f"{today}T00:00:00",
                                            "time_zone": "Europe/Kiev"
                                        }
                                    }
                                }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    },
                    "aggs": {
                        "by_ufilial": {
                            "terms": {
                                "field": "md.UFILIAL",
                                "size": 10000
                            },
                            "aggs": {
                                "timestamp": {
                                    "date_histogram": {
                                        "field": "timestamp",
                                        "fixed_interval": f"{interval}",
                                        "time_zone": "Europe/Kiev"
                                    },
                                    "aggs": {
                                        "model_min": {
                                            "avg": {
                                                "field": "model_lower.avg"
                                            }
                                        },
                                        "model_median": {
                                            "avg": {
                                                "field": "model_median.avg"
                                            }
                                        },
                                        "model_max": {
                                            "avg": {
                                                "field": "model_upper.avg"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )
        return body

    @staticmethod
    def get_body_cash_by_filials_plan(date_today, date_set, interval: str = '30m'):
        if isinstance(date_set, str):
            date = date_set
        else:
            date = date_set.strftime("%Y-%m-%d")
        today = date_today.strftime("%Y-%m-%d")
        body = json.dumps({
                    "size" : 0,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "timestamp": {
                                            "format": "strict_date_optional_time",
                                            "gte": f"{date}T00:00:00",
                                            "lt": f"{today}T00:00:00",
                                            "time_zone": "Europe/Kiev"
                                        }
                                    }
                                }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    },
                    "aggs": {
                        "by_ufilial": {
                            "terms": {
                                "field": "UFILIAL",
                                "size": 10000
                            },
                            "aggs": {
                                "time_histogram": {
                                    "date_histogram": {
                                        "field": "timestamp",
                                        "fixed_interval": f"{interval}",
                                        "time_zone": "Europe/Kiev"
                                    },
                                    "aggs": {
                                        "planned": {
                                            "avg": {
                                                "field": "total_needcashdesks"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )
        return body


    @staticmethod
    def get_body_sco_by_ufilial_plan(date_today, date_set, interval: str = '30m'):
        """interval: 1h or 30m, etc"""
        if isinstance(date_set, str):
            date = date_set
        else:
            date = date_set.strftime("%Y-%m-%d")
        today = date_today.strftime("%Y-%m-%d")
        body = json.dumps({
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "timestamp": {
                                            "format": "strict_date_optional_time",
                                            "gte": f"{date}T00:00:00",
                                            "lt": f"{today}T00:00:00",
                                            "time_zone": "Europe/Kiev"
                                        }
                                    }
                                }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    },
                    "aggs": {
                        "by_ufilial": {
                            "terms": {
                                "field": "md.UFILIAL",
                                "size": 10000
                            },
                            "aggs": {
                                "timestamp": {
                                    "date_histogram": {
                                        "field": "timestamp",
                                        "fixed_interval": f"{interval}",
                                        "time_zone": "Europe/Kiev"
                                    },
                                    "aggs": {
                                        "model_median": {
                                            "avg": {
                                                "field": "ml_median"
                                            }
                                        },
                                        "model_max": {
                                            "avg": {
                                                "field": "ml_max"
                                            }
                                        },
                                        "scocount": {
                                            "avg": {
                                                "field": "md.scocount"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
        return body

    @staticmethod
    def get_body_signature_servers(date_set_start, date_set_end):

        date_start = date_set_start.strftime("%Y-%m-%d")
        date_end = date_set_end.strftime("%Y-%m-%d")
        body = json.dumps({
                "size": 10000,
                "_source" : ["@timestamp", "msg.state", "msg.hostname", "msg.servicedesc"],
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": f"{date_start}T00:00:00",
                                        "lt": f"{date_end}T00:00:00",
                                        "time_zone": "Europe/Kiev"
                                    }
                                }
                            }
                        ],
                        "should": [],
                        "must_not": []
                    }
                }
            })
        return body

    @staticmethod
    def get_body_devices_total_by_filial(set_date = None):
        #date = set_date.strftime("%Y-%m-%d")
        body = json.dumps({
                        "size" : "10000",
                        "query" : {
                            "match_all" : {}
                            }
                        })
        return body

    @staticmethod
    def get_body_scales_paper_state(set_date_start, set_date_end, interval: str = '30m'):

        date_start = set_date_start if isinstance(set_date_start, str) else set_date_start.strftime("%Y-%m-%d")
        date_end = set_date_end if isinstance(set_date_end, str) else set_date_end.strftime("%Y-%m-%d")
        body = json.dumps({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": f"{date_start}T00:00:00",
                                    "lt": f"{date_end}T00:00:00",
                                    "time_zone": "Europe/Kiev"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "by_ufilial": {
                    "terms": {
                        "field": "UFILIAL",
                        "size": 10000
                    },
                    "aggs": {
                        "time_histogram": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": interval,
                                "time_zone": "Europe/Kiev"
                            },
                            "aggs": {
                                "ok_p": {
                                    "avg": {
                                        "field": "ok_p"
                                    }
                                },
                                "ok_np": {
                                    "avg": {
                                        "field": "ok_np"
                                    }
                                },
                                "ml_median": {
                                    "avg": {
                                        "field": "ml_median"
                                    }
                                },
                                "Tags": {
                                    "top_hits": {
                                        "size": 1,
                                        "_source": {
                                            "includes": [
                                                "md.businessid"
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })

        return body