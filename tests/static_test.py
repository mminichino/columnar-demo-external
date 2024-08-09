#!/usr/bin/env python
#

import argparse
import warnings
import os
import sys

warnings.filterwarnings("ignore")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from columnardemo.columnar_driver import CBSession

top_spender_query = """
SELECT c.name, SUM(amt.amount) AS total_spend
FROM `customers` c
JOIN `accounts` a ON ANY acc IN c.accounts SATISFIES acc = a.account_id END
JOIN `transactions` t ON a.account_id = t.account_id
UNNEST t.transactions AS amt
GROUP BY c.name
ORDER BY total_spend DESC
LIMIT 10
"""


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
    parser.add_argument('-b', '--bucket', action='store', help="Bucket", default="cbdocs")
    parser.add_argument('-s', '--scope', action='store', help="Scope", default="_default")
    options = parser.parse_args()
    return options


def main():
    options = parse_args()

    session = CBSession(options.host, options.user, options.password).session().bucket_name(options.bucket).scope_name(options.scope)
    results = session.analytics_query(top_spender_query)
    print(results)


if __name__ == '__main__':
    main()
