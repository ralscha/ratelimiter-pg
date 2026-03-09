package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	ratelimit "github.com/ralscha/ratelimiter-pg"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

type loginHandler struct {
	Limiter *ratelimit.RateLimiter
	Config  ratelimit.BucketConfig
}

func (h *loginHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.writeJSON(w, http.StatusMethodNotAllowed, loginResponse{OK: false, Message: "use POST"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, loginResponse{OK: false, Message: "invalid json payload"})
		return
	}

	username := strings.TrimSpace(req.Username)
	if username == "" {
		h.writeJSON(w, http.StatusBadRequest, loginResponse{OK: false, Message: "username is required"})
		return
	}

	decision, err := h.Limiter.Allow(r.Context(), loginKey(username), h.Config)
	if err != nil {
		log.Printf("rate limit error: %v", err)
		h.writeJSON(w, http.StatusInternalServerError, loginResponse{OK: false, Message: "internal error"})
		return
	}

	if !decision.Allowed {
		retrySeconds := max(int(math.Ceil(decision.RetryAfter.Seconds())), 1)
		w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
		h.writeJSON(w, http.StatusTooManyRequests, loginResponse{OK: false, Message: "too many login attempts"})
		return
	}

	h.writeJSON(w, http.StatusAccepted, loginResponse{OK: true, Message: "login accepted for verification"})
}

func (h *loginHandler) writeJSON(w http.ResponseWriter, status int, payload loginResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write response: %v", err)
	}
}

func loginKey(username string) string {
	user := strings.ToLower(strings.TrimSpace(username))
	return "login:user:" + user
}
