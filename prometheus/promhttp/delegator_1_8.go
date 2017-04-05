// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2013, The Prometheus Authors
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// +build go1.8

package promhttp

import (
	"io"
	"net/http"
)

func newDelegator(w http.ResponseWriter) delegator {
	d := &responseWriterDelegator{ResponseWriter: w}

	_, cn := w.(http.CloseNotifier)
	_, fl := w.(http.Flusher)
	_, hj := w.(http.Hijacker)
	_, ps := w.(http.Pusher)
	_, rf := w.(io.ReaderFrom)
	if cn && fl && hj && rf && ps {
		return &fancyResponseWriterDelegator{d}
	}

	return d
}

func (f *fancyResponseWriterDelegator) Push(target string, opts *http.PushOptions) error {
	return f.ResponseWriter.(http.Pusher).Push(target, opts)
}