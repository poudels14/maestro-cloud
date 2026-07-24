use kernel_store::ExpectedVersion;

pub(super) enum WritePlan<Resource> {
    Retain {
        resource: Resource,
        expected: ExpectedVersion,
    },
    Put {
        resource: Resource,
        expected: ExpectedVersion,
    },
}
