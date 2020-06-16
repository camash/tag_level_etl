#!/usr/bin/python3
# -*- coding: UTF-8 -*-


class FileloadError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
