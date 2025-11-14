#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from itertools import cycle

# KOFIC_API_KEY, KOFIC_API_KEY_2, KOFIC_API_KEY_3, ... KOFIC_API_KEY_10 까지 스캔
def get_api_keys_from_env():
    """
    환경 변수에서 KOFIC_API_KEY, KOFIC_API_KEY_2, ... KOFIC_API_KEY_10 까지
    모든 유효한 키를 읽어와 리스트로 반환합니다.
    """
    keys = []
    base_key = os.environ.get("KOFIC_API_KEY", "").strip()
    if base_key:
        keys.append(base_key)
    
    for i in range(2, 11): # _2 부터 _10 까지
        key = os.environ.get(f"KOFIC_API_KEY_{i}", "").strip()
        if key:
            keys.append(key)
            
    if not keys:
        print("[kofic_api] 경고: KOFIC_API_KEY가 설정되지 않았습니다.")
        
    print(f"[kofic_api] 총 {len(keys)}개의 API 키를 로드했습니다.")
    return keys

class KeyRotator:
    """
    API 키 목록을 받아, API 호출(세션 생성) 시마다 키를 자동으로 교체(rotate)합니다.
    """
    def __init__(self, keys: list[str]):
        if not keys:
            raise ValueError("API 키가 최소 1개 이상 필요합니다.")
        self.keys = keys
        self.key_cycle = cycle(keys) # 키를 무한히 순환
        self.headers = {"User-Agent": "cache-builder/1.1"}

    def get_next_key(self) -> str:
        """다음 API 키를 반환합니다."""
        return next(self.key_cycle)

    def get_session_with_key(self) -> tuple[requests.Session, str]:
        """
        다음 API 키가 포함된, 자동 재시도 기능의 requests.Session을 반환합니다.
        (세션, 사용된 키) 튜플을 반환합니다.
        """
        s = requests.Session()
        retries = Retry(
            total=8, 
            connect=5, 
            read=5, 
            backoff_factor=1.5, 
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update(self.headers)
        
        key = self.get_next_key()
        return s, key

# 스크립트 로드 시점에 키 로테이터를 즉시 생성
API_KEYS = get_api_keys_from_env()
# API_KEYS 리스트가 비어있으면(로컬 테스트 등) KeyRotator 생성이 실패하므로,
# 임시 키를 넣어 예외를 방지합니다.
KEY_ROTATOR = KeyRotator(API_KEYS if API_KEYS else ["dummy_key"])

def get_session() -> tuple[requests.Session, str]:
    """공용: 다음 API 키로 세션을 가져옵니다."""
    if not API_KEYS:
        raise RuntimeError("KOFIC API 키가 없습니다. GitHub Secrets를 확인하세요.")
    return KEY_ROTATOR.get_session_with_key()
