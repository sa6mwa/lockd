package cryptoutil

import (
	"fmt"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
)

const (
	// MetadataDescriptorName identifies the global metadata descriptor embedded in CA and server bundles.
	MetadataDescriptorName = "lockd/metadata"
)

// CACertificateID returns the kryptograf certificate identifier for the first certificate
// found in the provided PEM data.
func CACertificateID(caPEM []byte) (string, error) {
	if len(caPEM) == 0 {
		return "", fmt.Errorf("compute ca id: empty certificate bundle")
	}
	ids, err := kryptograf.CertificateIDsFromPEM(caPEM)
	if err != nil {
		return "", fmt.Errorf("compute ca id: %w", err)
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("compute ca id: no certificates found")
	}
	return ids[0], nil
}

// MetadataMaterial bundles the root key and descriptor required to encrypt metadata.
type MetadataMaterial struct {
	Root       keymgmt.RootKey
	Descriptor keymgmt.Descriptor
}

// EnsureCAMetadataMaterial loads the CA bundle at path, ensuring a root key and metadata descriptor exist.
// The descriptor is derived using the CA certificate identifier as kryptograf context.
func EnsureCAMetadataMaterial(caPath string, caCertPEM []byte) (MetadataMaterial, error) {
	store, err := keymgmt.LoadPEM(caPath)
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("load ca bundle for kryptograf material: %w", err)
	}
	root, err := store.EnsureRootKey()
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("ensure ca root key: %w", err)
	}
	caID, err := CACertificateID(caCertPEM)
	if err != nil {
		return MetadataMaterial{}, err
	}
	mat, err := store.EnsureDescriptor(MetadataDescriptorName, root, []byte(caID))
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("ensure ca descriptor: %w", err)
	}
	if err := store.Commit(); err != nil {
		return MetadataMaterial{}, fmt.Errorf("commit ca kryptograf material: %w", err)
	}
	return MetadataMaterial{
		Root:       root,
		Descriptor: mat.Descriptor,
	}, nil
}

// MetadataMaterialFromBundle extracts the metadata material from the PEM bundle at path.
func MetadataMaterialFromBundle(path string) (MetadataMaterial, error) {
	store, err := keymgmt.LoadPEM(path)
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("load bundle for kryptograf material: %w", err)
	}
	return metadataMaterialFromStore(store)
}

// MetadataMaterialFromBytes extracts metadata material from PEM content supplied as bytes.
func MetadataMaterialFromBytes(data []byte) (MetadataMaterial, error) {
	store, err := keymgmt.LoadPEM(data)
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("load bundle bytes for kryptograf material: %w", err)
	}
	return metadataMaterialFromStore(store)
}

// ApplyMetadataMaterial injects the provided material into the PEM payload and returns updated bytes.
func ApplyMetadataMaterial(pemBytes []byte, material MetadataMaterial) ([]byte, error) {
	var out []byte
	store, err := keymgmt.LoadPEMInto(pemBytes, &out)
	if err != nil {
		return nil, fmt.Errorf("prepare bundle for kryptograf material: %w", err)
	}
	store.SetRootKey(material.Root)
	if err := store.SetDescriptor(MetadataDescriptorName, material.Descriptor); err != nil {
		return nil, fmt.Errorf("set bundle descriptor: %w", err)
	}
	if err := store.Commit(); err != nil {
		return nil, fmt.Errorf("commit bundle kryptograf material: %w", err)
	}
	if len(out) == 0 {
		return pemBytes, nil
	}
	return out, nil
}

func metadataMaterialFromStore(store keymgmt.Store) (MetadataMaterial, error) {
	root, ok, err := store.RootKey()
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("read bundle root key: %w", err)
	}
	if !ok {
		return MetadataMaterial{}, fmt.Errorf("bundle missing kryptograf root key")
	}
	desc, ok, err := store.Descriptor(MetadataDescriptorName)
	if err != nil {
		return MetadataMaterial{}, fmt.Errorf("read bundle descriptor: %w", err)
	}
	if !ok {
		return MetadataMaterial{}, fmt.Errorf("bundle missing descriptor %q", MetadataDescriptorName)
	}
	return MetadataMaterial{
		Root:       root,
		Descriptor: desc,
	}, nil
}
