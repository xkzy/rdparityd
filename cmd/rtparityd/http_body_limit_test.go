/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPMaxBytesEnforcement(t *testing.T) {
	maxBytes := int64(1024)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := http.MaxBytesReader(w, r.Body, maxBytes)
		data, err := io.ReadAll(body)
		if err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, strings.Repeat("x", len(data)))
	})

	t.Run("request within limit", func(t *testing.T) {
		body := strings.NewReader(strings.Repeat("a", 512))
		req := httptest.NewRequest("POST", "/", body)
		rr := httptest.NewRecorder()
		handler(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}
	})

	t.Run("request exceeds limit", func(t *testing.T) {
		body := strings.NewReader(strings.Repeat("a", 2048))
		req := httptest.NewRequest("POST", "/", body)
		rr := httptest.NewRecorder()
		handler(rr, req)
		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("expected status 413, got %d", rr.Code)
		}
	})
}
