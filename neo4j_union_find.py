# encoding: utf-8

"""
Linking local userids with Neo4j.

This code implements the Union-Find data structure on top of the Neo4j graph
database.  Tested in Python 3.

Union-find data structure based on code by Josiah Carlson
(http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912)

and D. Eppstein:
(http://www.ics.uci.edu/~eppstein/PADS/UnionFind.py)

:author: Robert David Grant <robert.david.grant@gmail.com>

:copyright:
    Copyright 2017 Robert David Grant.

    Licensed under the Apache License, Version 2.0 (the "License"); you
    may not use this file except in compliance with the License.  You
    may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    implied.  See the License for the specific language governing
    permissions and limitations under the License.
"""

import os
from operator import itemgetter
from uuid import uuid4
from py2neo import Graph, Node, NodeSelector, Relationship


class HasParent(Relationship):
    pass


class UnionFind:
    """Union-find data structure in Neo4j.

    Maintains disjoint sets of local_ids in Neo4j and supports the
    `find` and `union` operations.
    """

    def __init__(self, graph):
        """Create a new empty union-find structure from a py2neo.Graph"""
        self.graph = graph
        self.select = NodeSelector(graph).select

    def _set_parent(self, node, parent):
        """Set `node`'s parent to `parent`."""
        parent_relation = self.graph.match_one(start_node=node,
                                               rel_type="HAS_PARENT")
        self.graph.separate(parent_relation)
        self.graph.create(HasParent(node, parent))

    def find(self, type, name):
        """Find and return the root of the set containing the object of type
        `type` and local name `name`.

        Each set is rooted by an arbitrarily-chosen one of its members; as long
        as the set remains unchanged it will keep the same name.  If the item
        is not yet part of a set, a new singleton set is created for it.
        """
        existing = self.select(type=type, name=name).first()
        if existing is None:
            node = Node(id=uuid4().hex, type=type, name=name, weight=1)
            relationship = HasParent(node, node)
            self.graph.create(node)
            self.graph.create(relationship)
            return node

        # root's parent is root
        is_root = self.graph.match_one(start_node=existing,
                                       rel_type="HAS_PARENT",
                                       end_node=existing) is not None
        if is_root:
            return existing
        else:  # else, find root (which points to itself)
            root = self.graph.data("match ({id: '%s'})"
                                   "-[:HAS_PARENT*]->(r)-[:HAS_PARENT]->(r) "
                                   "return r" % existing['id'])[0]['r']

        # compress paths and return
        # TODO: this pattern actually matches all ancestors of root, not just
        # the path from existing to root
        ancestors = self.graph.run("match (r {id: '%s'})"
                                   "match (a)-[:HAS_PARENT*]->(r)"
                                   "return a" % root['id'])
        ancestors = (a['a'] for a in ancestors)
        for ancestor in ancestors:
            self._set_parent(ancestor, root)

        return root
        
    def union(self, objects):
        """Find the sets containing objects and merge them all.

        Merges the sets containing each item into a single larger set.  If any
        item is not yet part of a set, it is added as one of the members of the
        merged set.

        Parameters
        ----------
        objects : iterable of (str, str)
            Each tuple represents an object with (local_id_type, local_id_name)
        """
        roots = [self.find(type=x[0], name=x[1]) for x in objects]
        heaviest = max(roots, key=itemgetter('weight'))
        for r in roots:
            if r['id'] != heaviest['id']:
                heaviest['weight'] += r['weight']
                self.graph.push(heaviest)
                self._set_parent(r, heaviest)

    def union_from_stream(self, stream):
        """Build up UnionFind data structure from stream of rows of local_ids.
        
        Parameters
        ----------
        stream: iterable of dicts of (local_id_type: local_id_name)
        """
        for row in stream:
            matches = filter(itemgetter(1), row.items())  # filter out missing
            self.union(matches)

    def global_id(self, type, name):
        return self.find(type, name)['id']


def test():
    user = os.environ['NEO4J_USER']
    password = os.environ['NEO4J_PASSWORD']
    uri = "http://localhost:7474"

    graph = Graph(uri, user=user, password=password)
    ids = UnionFind(graph)

    return ids
