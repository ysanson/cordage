package distribute

import (
	"errors"
	"net"
	"reflect"
	"testing"
)

func TestStaticDiscoverer(t *testing.T) {
	want := []string{"127.0.0.1:9000", "127.0.0.1:9001"}
	got, err := StaticDiscoverer(want)()
	if err != nil {
		t.Fatalf("StaticDiscoverer: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDNSDiscoverer(t *testing.T) {
	t.Cleanup(func() { lookupHost = net.LookupHost })

	lookupHost = func(host string) ([]string, error) {
		if host != "cordage-worker-headless" {
			t.Fatalf("unexpected host %q", host)
		}
		// Deliberately unsorted, to exercise the sort.
		return []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, nil
	}

	got, err := DNSDiscoverer("cordage-worker-headless", 50051)()
	if err != nil {
		t.Fatalf("DNSDiscoverer: %v", err)
	}
	want := []string{"10.0.0.1:50051", "10.0.0.2:50051", "10.0.0.3:50051"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDNSDiscovererResolveError(t *testing.T) {
	t.Cleanup(func() { lookupHost = net.LookupHost })

	wantErr := errors.New("no such host")
	lookupHost = func(string) ([]string, error) { return nil, wantErr }

	_, err := DNSDiscoverer("cordage-worker-headless", 50051)()
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want wrapping %v", err, wantErr)
	}
}

func TestDNSDiscovererEmptyResult(t *testing.T) {
	t.Cleanup(func() { lookupHost = net.LookupHost })

	lookupHost = func(string) ([]string, error) { return nil, nil }

	_, err := DNSDiscoverer("cordage-worker-headless", 50051)()
	if err == nil {
		t.Fatal("expected an error for zero resolved addresses, got nil")
	}
}
