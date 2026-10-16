"""IP to ASN and IP to IXP mappings based on the Internet Yellow Pages (IYP).

Prefix to ASN mappings come from the BGPKIT pfx2as dataset (BGP data from
RouteViews and RIPE RIS) and IXP peering LANs come from the PeeringDB ix
dataset. Both are fetched from IYP and cached on disk (see tools.py), so that
no local RIB dump or IXP file is needed.

This module provides the same interface as ip2asn.py, hence it can be used as
a drop-in replacement:
    i2a = iyp2asn.ip2asn()
    i2a.ip2asn('8.8.8.8')

As in ip2asn.py, IXPs are reported with negative AS numbers. The absolute
value of these numbers is the PeeringDB ID of the IXP.
"""

import logging
import sys

import radix

import tools


class ip2asn():
    """Map IP addresses to ASNs and IXPs using IYP data."""

    def __init__(self, iyp_url=None, prefix_asn=None, ixp_prefixes=None):
        """Load prefixes from IYP (or the local cache) in a radix tree.

        prefix_asn and ixp_prefixes can be given to use already fetched data,
        they are respectively lists of [prefix, asn] and
        [prefix, peeringdb_ix_id, ixp_name] values.
        """

        self.asname = {}
        self.rtree = radix.Radix()

        if prefix_asn is None:
            prefix_asn = tools.get_prefix_asn(iyp_url=iyp_url)

        if ixp_prefixes is None:
            ixp_prefixes = tools.get_ixp_prefixes(iyp_url=iyp_url)

        nb_asn_prefixes = 0
        for prefix, asn in prefix_asn:
            node = self.add_prefix(prefix)
            if node is None:
                continue

            # Prefixes originated by several ASes (MOAS) are reported once per
            # AS, keep only the first one
            if "as" not in node.data:
                node.data["as"] = int(asn)
                nb_asn_prefixes += 1

        # IXP peering LANs are added last, they take precedence over the
        # prefixes seen in BGP
        nb_ixp_prefixes = 0
        for prefix, ixp_id, name in ixp_prefixes:
            node = self.add_prefix(prefix)
            if node is None:
                continue

            # negative values for IXPs
            asn = int(ixp_id)*-1
            node.data["as"] = asn
            self.asname[asn] = name
            nb_ixp_prefixes += 1

        logging.info("Loaded {} prefixes from IYP ({} from BGP, {} IXP "
                     "peering LANs)".format(len(self.rtree.prefixes()),
                                            nb_asn_prefixes, nb_ixp_prefixes))

    def add_prefix(self, prefix):
        """Add a prefix to the radix tree and return the corresponding node,
        or None if the prefix is invalid."""

        try:
            return self.rtree.add(prefix)
        except (ValueError, TypeError):
            logging.warning("Ignoring invalid prefix: {}".format(prefix))
            return None

    def ip2asn(self, ip):
        """ Find the ASN corresponding to the given IP address"""

        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return 0

        if node is None:
            return 0
        else:
            return node.data.get("as", 0)

    def covering_prefixes(self, ip):
        """ Find all prefixes covering the given IP address"""

        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return None

        if node is None:
            return None

        parents = [node.prefix]
        parent = node.parent
        while parent:
            parents.append(parent.prefix)
            parent = parent.parent

        return parents

    def ip2prefix(self, ip):

        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return None

        if node is None:
            return None
        else:
            return node.prefix

    def asn2name(self, asn):

        if asn in self.asname:
            return self.asname[asn]
        else:
            return ""


if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    if len(sys.argv) < 2:
        print("usage: %s ip [ip ...]" % sys.argv[0])
        sys.exit()

    ia = ip2asn()
    for ip in sys.argv[1:]:
        asn = ia.ip2asn(ip)
        prefix = ia.ip2prefix(ip)
        if not asn:
            print("{}: Unknown".format(ip))
        elif asn < 0:
            print("{}: {} IXP {} ({})".format(
                ip, prefix, asn*-1, ia.asn2name(asn)))
        else:
            print("{}: {} AS{}".format(ip, prefix, asn))

