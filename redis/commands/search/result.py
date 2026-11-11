from typing import List, Optional

from redis.utils import decode_field_value, str_if_bytes

from ._util import to_string
from .document import Document


def _make_document(doc_id, fields, score, with_scores, payload, has_payload):
    # A document field named "payload" or "score" is passed through **fields,
    # so it must not also be passed as a keyword. The reply's own score and
    # payload take precedence when they were requested, as the id does.
    doc = Document(doc_id, **fields)
    if with_scores:
        doc.score = score
    if has_payload:
        doc.payload = payload
    return doc


class Result:
    """
    Represents the result of a search query, and has an array of Document
    objects
    """

    def __init__(
        self,
        res,
        hascontent,
        duration=0,
        has_payload=False,
        with_scores=False,
        field_encodings: Optional[dict] = None,
        warnings: Optional[List[str]] = None,
    ):
        """
        - duration: the execution time of the query
        - has_payload: whether the query has payloads
        - with_scores: whether the query has scores
        - field_encodings: a dictionary of field encodings if any is provided
        - warnings: list of server warnings (from RESP3 responses)
        """

        self.total = res[0]
        self.duration = duration
        self.docs = []
        self.warnings = warnings or []

        step = 1
        if hascontent:
            step = step + 1
        if has_payload:
            step = step + 1
        if with_scores:
            step = step + 1

        offset = 2 if with_scores else 1

        for i in range(1, len(res), step):
            id = to_string(res[i])
            payload = to_string(res[i + offset]) if has_payload else None
            # fields_offset = 2 if has_payload else 1
            fields_offset = offset + 1 if has_payload else offset
            score = float(res[i + 1]) if with_scores else None

            fields = {}
            if hascontent and res[i + fields_offset] is not None:
                keys = map(to_string, res[i + fields_offset][::2])
                values = res[i + fields_offset][1::2]

                for key, value in zip(keys, values):
                    if field_encodings is None or key not in field_encodings:
                        fields[key] = to_string(value)
                        continue

                    encoding = field_encodings[key]

                    # If the encoding is None, we don't need to decode the value
                    if encoding is None:
                        fields[key] = value
                    else:
                        fields[key] = to_string(value, encoding=encoding)

            try:
                del fields["id"]
            except KeyError:
                pass

            try:
                fields["json"] = fields["$"]
                del fields["$"]
            except KeyError:
                pass

            self.docs.append(
                _make_document(id, fields, score, with_scores, payload, has_payload)
            )

    @classmethod
    def from_resp3(
        cls,
        res: dict,
        duration: float = 0,
        with_scores: bool = False,
        field_encodings: Optional[dict] = None,
        with_payloads: bool = False,
    ) -> "Result":
        """Construct a Result from a RESP3 dict response.

        RESP3 format:
        {
            "total_results": N,
            "results": [
                {"id": "doc1", "score": 1.5, "extra_attributes": {"f1": "v1"}},
                ...
            ],
            "warning": [...]
        }
        """
        instance = cls.__new__(cls)
        if res is None:
            res = {}
        # On RESP3 connections with decode_responses=False the server's map
        # keys arrive as bytes, so normalise them to strings before lookup
        # to keep behaviour consistent with decode_responses=True.
        res = {str_if_bytes(k): v for k, v in res.items()}
        instance.total = res.get("total_results", 0)
        instance.duration = duration
        instance.docs = []
        instance.warnings = [str_if_bytes(w) for w in res.get("warning", [])]

        for result_item in res.get("results", []):
            result_item = {str_if_bytes(k): v for k, v in result_item.items()}
            doc_id = str_if_bytes(result_item.get("id", ""))
            score = None
            if with_scores and "score" in result_item:
                score = float(result_item["score"])
            payload = None
            if with_payloads:
                payload = to_string(result_item.get("payload"))

            fields = {}
            extra_attrs = result_item.get("extra_attributes") or {}
            for key, value in extra_attrs.items():
                key = str_if_bytes(key)
                fields[key] = decode_field_value(value, key, field_encodings)

            try:
                del fields["id"]
            except KeyError:
                pass

            try:
                fields["json"] = fields["$"]
                del fields["$"]
            except KeyError:
                pass

            instance.docs.append(
                _make_document(
                    doc_id, fields, score, with_scores, payload, with_payloads
                )
            )

        return instance

    def __repr__(self) -> str:
        return f"Result{{{self.total} total, docs: {self.docs}}}"
