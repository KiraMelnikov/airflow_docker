import json


class ServicesData:

    @staticmethod
    def get_cyber_security_url():
        return "https://sfpv-elas001.fz.fozzy.lan:9200/dkib-findings*/_search"


    @staticmethod
    def get_cyber_security_incidents_url(get_by: str = None, year: str = None, size: int = None):
        if year and size:
            url = f"https://sfpv-qrad001.fz.fozzy.lan/console/plugins/1303/app_proxy:main/api/incidents?sort=date&size={size}&year={year}"
        else:
            if get_by:
                if size:
                    url = f"https://sfpv-qrad001.fz.fozzy.lan/console/plugins/1303/app_proxy:main/api/incidents?sort=date&getBy={get_by}&size={size}"
                else:
                    url = f"https://sfpv-qrad001.fz.fozzy.lan/console/plugins/1303/app_proxy:main/api/incidents?sort=date&getBy={get_by}"
            else:
                raise AttributeError(f"Attributes is not defined")
        return url


    @staticmethod
    def get_azure_idp_url():
        return "https://fozzyb2c.b2clogin.com/fozzyb2c.onmicrosoft.com/b2c_1_prod/oauth2/v2.0/token"


    @staticmethod
    def get_elkasa_url(business: str, endpoint: str, params: dict) -> str:
        assert isinstance(business, str)
        assert isinstance(endpoint, str)
        url = f"https://{business}.elkasa.com.ua/api"
        iterable = [f"{key}={value}" for key, value in params.items()]
        params = '&'.join(iterable)
        return url + endpoint + "?" + params


    @staticmethod
    def get_cyber_security_body():
        return json.dumps({
            "size": 0,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "bool": {
                                "filter": [
                                    {
                                        "bool": {
                                            "should": [
                                                {
                                                    "match_phrase": {
                                                        "RecordState": "ACTIVE"
                                                    }
                                                }
                                            ],
                                            "minimum_should_match": 1
                                        }
                                    },
                                    {
                                        "bool": {
                                            "should": [
                                                {
                                                    "match_phrase": {
                                                        "ProductName": "Inspector"
                                                    }
                                                }
                                            ],
                                            "minimum_should_match": 1
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": "now-24h"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": [
                        {
                            "match_phrase": {
                                "account_name.keyword": "haha"
                            }
                        },
                        {
                            "match_phrase": {
                                "FindingProviderFields.Types": "Software and Configuration Checks/AWS Security Best Practices/Network Reachability - Recognized port reachable from a Peered VPC"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "Account": {
                    "terms": {
                        "field": "Resources.Tags.AccountName.keyword",
                        "size": 1000
                    },
                    "aggs": {
                        "Severity": {
                            "filters": {
                                "filters": {
                                    "Critical": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "CRITICAL"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    },
                                    "High": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "HIGH"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    },
                                    "Medium": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "MEDIUM"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    }
                                }
                            }
                        },
                        "Severity_Old": {
                            "filters": {
                                "filters": {
                                    "Critical_Old": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "CRITICAL"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "FirstObservedAt": {
                                                            "lte": "now-30d/d"
                                                        }
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    },
                                    "High_Old": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "HIGH"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "FirstObservedAt": {
                                                            "lte": "now-30d/d"
                                                        }
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    },
                                    "Medium_Old": {
                                        "bool": {
                                            "must": [],
                                            "filter": [
                                                {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "Severity.Label.keyword": "MEDIUM"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "FirstObservedAt": {
                                                            "lte": "now-30d/d"
                                                        }
                                                    }
                                                }
                                            ],
                                            "should": [],
                                            "must_not": []
                                        }
                                    }
                                }
                            }
                        },
                        "Tags": {
                            "top_hits": {
                                "size": 1,
                                "_source": {
                                    "includes": ["Resources.Tags.BU", "Resources.Tags.Environment", "Resources.Tags.Project", "Resources.Tags.ProjectManager", "Resources.Tags.TechnicalOwner", "FirstObservedAt", "LastObservedAt", "AwsAccountId"]
                                },
                                "sort": [
                                    {
                                        "@timestamp": {
                                            "order": "desc"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }
        )

    @staticmethod
    def get_azure_idp_body(AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, DESTINATION_CLIENT_ID):
        return {
            "client_id": AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": "https://FozzyB2C.onmicrosoft.com/{}/.default".format(DESTINATION_CLIENT_ID)
        }