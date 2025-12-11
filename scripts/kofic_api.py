#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from itertools import cycle

# [수정] 7번째 키 추가
KEY_NAMES = [
    "KOFIC_API_KEY",
    "KOFIC_API_KEY_2",
    "KOFIC_API_KEY_3",
    "KOFIC_API_KEY_4",
    "KOFIC_API_KEY_5",
    "KOFIC_API_KEY_6",
    "KOFIC_API_KEY_7"
]

class KoficApiRotator:
    def __init__(self):
        self.keys = []
        for name in KEY_NAMES:
            val = os.environ.get(name, "").strip()
            if val:
                self.keys.append(val)
        
        if not self.keys:
            self.keys = ["DUMMY_KEY"]
            
        self.key_cycle = cycle(self.keys)
        self.current_key = next(self.key_cycle)
        self.session = self._make_session()
        print(f"[kofic_api] 로드된 API 키 개수: {len(self.keys)}개")

    def _make_session(self):
        s = requests.Session()
        retries = Retry(total=5, connect=3, read=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def get_key(self):
        return self.current_key

    def rotate_key(self):
        """다음 키로 교체"""
        prev = self.current_key
        self.current_key = next(self.key_cycle)
        print(f"[kofic_api] 키 교체! ({prev[:4]}... -> {self.current_key[:4]}...)")
        return self.current_key

    def request(self, url, params=None, timeout=30):
        if params is None: params = {}
        
        max_retries = len(self.keys) + 1
        for i in range(max_retries):
            params['key'] = self.current_key
            try:
                r = self.session.get(url, params=params, timeout=timeout)
                data = r.json()
                
                fault = data.get("faultInfo") or data.get("faultResult")
                if fault:
                    msg = fault.get("message", "")
                    code = fault.get("errorCode", "")
                    if code in ["320010", "320011"] or "limit" in msg.lower():
                        print(f"[kofic_api] 키 소진됨 ({self.current_key[:4]}...). 교체 시도.")
                        self.rotate_key()
                        time.sleep(1)
                        continue 
                    else:
                        return data
                return data

            except Exception as e:
                print(f"[kofic_api] 통신 오류: {e}")
                time.sleep(2)
        
        raise Exception("모든 API 키를 시도했으나 실패했습니다.")

rotator = KoficApiRotator()

def get_session():
    return rotator.session, rotator.current_key

def fetch(url, params=None):
    return rotator.request(url, params)

API_KEYS = rotator.keys
