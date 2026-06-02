package aibot

import "testing"

func TestWecomCryptoEncryptDecryptRoundTrip(t *testing.T) {
	crypto, err := NewWecomCrypto("token", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG", "bot-1")
	if err != nil {
		t.Fatalf("NewWecomCrypto returned error: %v", err)
	}

	encrypted, err := crypto.Encrypt("hello", "1710000000", "nonce")
	if err != nil {
		t.Fatalf("Encrypt returned error: %v", err)
	}
	if encrypted.Encrypt == "" || encrypted.Signature == "" {
		t.Fatalf("encrypted = %#v, want encrypt and signature", encrypted)
	}
	if !crypto.VerifySignature(encrypted.Signature, "1710000000", "nonce", encrypted.Encrypt) {
		t.Fatalf("signature should verify")
	}

	plain, err := crypto.Decrypt(encrypted.Encrypt)
	if err != nil {
		t.Fatalf("Decrypt returned error: %v", err)
	}
	if plain != "hello" {
		t.Fatalf("plain = %q, want hello", plain)
	}
}

func TestWecomCryptoRejectsReceiveIDMismatch(t *testing.T) {
	crypto1, err := NewWecomCrypto("token", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG", "bot-1")
	if err != nil {
		t.Fatalf("NewWecomCrypto returned error: %v", err)
	}
	encrypted, err := crypto1.Encrypt("hello", "1710000000", "nonce")
	if err != nil {
		t.Fatalf("Encrypt returned error: %v", err)
	}

	crypto2, err := NewWecomCrypto("token", "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG", "bot-2")
	if err != nil {
		t.Fatalf("NewWecomCrypto returned error: %v", err)
	}
	_, err = crypto2.Decrypt(encrypted.Encrypt)
	if err == nil {
		t.Fatalf("Decrypt should reject receive id mismatch")
	}
}
