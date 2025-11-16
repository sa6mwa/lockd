package querydata

import "testing"

func TestVoucherProfileFiltering(t *testing.T) {
	for _, vr := range voucherRecords {
		if vr.MinProfile > DatasetFull && vr.allowed(DatasetFull) {
			t.Fatalf("extended voucher %s allowed in full profile", vr.Key)
		}
	}
}
