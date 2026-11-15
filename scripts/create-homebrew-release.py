#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.13"
# dependencies = ["pygithub>=2.10.0,<3"]
# ///

import base64
import os
import sys
from typing import NamedTuple

from github import Auth, Github, GithubIntegration

TEMPLATE = """# This file was auto-generated. DO NOT EDIT.
cask "ver" do
  name "ver"
  desc "ver: a simple, non-git-like version control system"
  homepage "https://ver-vcs.net"
  version "[[VERSION]]"

  livecheck do
    skip "Auto-generated on release."
  end

  on_macos do
    on_intel do
      url "https://github.com/AstraBert/ver/releases/download/v#{version}/ver-v#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "[[SHA_APPLE_AMD]]"
      binary "ver-v#{version}-x86_64-apple-darwin/ver"
    end
    on_arm do
      url "https://github.com/AstraBert/ver/releases/download/v#{version}/ver-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "[[SHA_APPLE_ARM]]"
      binary "ver-v#{version}-aarch64-apple-darwin/ver"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/AstraBert/ver/releases/download/v#{version}/ver-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "[[SHA_LINUX_AMD]]"
      binary "ver-v#{version}-x86_64-unknown-linux-gnu/ver"
    end
    on_arm do
      url "https://github.com/AstraBert/ver/releases/download/v#{version}/ver-v#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "[[SHA_LINUX_ARM]]"
      binary "ver-v#{version}-aarch64-unknown-linux-gnu/ver"
    end
  end

  postflight do
    if OS.mac?
        system_command "/usr/bin/xattr",
            args: ["-dr", "com.apple.quarantine", "#{staged_path}/ver-v#{version}-#{Hardware::CPU.arm? ? "aarch64" : "x86_64"}-apple-darwin/ver"]
    end
  end

  # No zap stanza required
end
"""


class SHAs(NamedTuple):
    apple_amd: str
    apple_arm: str
    linux_amd: str
    linux_arm: str

    @classmethod
    def from_raw(cls, args: list[str]) -> "SHAs":
        if len(args) != 4:
            raise ValueError(
                "The provided list of SHAs has to contain exactly 4 elements"
            )
        return cls(
            apple_amd=args[0], apple_arm=args[1], linux_amd=args[2], linux_arm=args[3]
        )


def get_gh() -> Github:
    private_key = os.getenv("GITHUB_PRIVATE_KEY")
    if private_key is None:
        raise RuntimeError("Missing GITHUB_PRIVATE_KEY")
    app_id = os.getenv("GITHUB_APP_ID")
    if app_id is None:
        raise RuntimeError("Missing GITHUB_APP_ID")
    auth = Auth.AppAuth(
        app_id=int(app_id), private_key=base64.b64decode(private_key).decode("utf-8")
    )
    gi = GithubIntegration(auth=auth)
    gh = gi.get_installations()[0].get_github_for_installation()
    return gh


def generate_cask_file(version: str, shas: SHAs) -> tuple[str, str]:
    filled = (
        TEMPLATE.replace("[[VERSION]]", version)
        .replace("[[SHA_APPLE_ARM]]", shas.apple_arm)
        .replace("[[SHA_APPLE_AMD]]", shas.apple_amd)
        .replace("[[SHA_LINUX_AMD]]", shas.linux_amd)
        .replace("[[SHA_LINUX_ARM]]", shas.linux_arm)
    )
    return (filled, f"release: v{version}")


def main() -> None:
    argv = sys.argv
    if len(argv) != 6:
        print(argv)
        print("This script needs exactly 5 arguments")
        sys.exit(2)
    gh = get_gh()
    cask, msg = generate_cask_file(argv[1], SHAs.from_raw(argv[2:]))
    repo = gh.get_repo("AstraBert/homebrew-ver")
    content = repo.get_contents("Casks/ver.rb")
    if isinstance(content, list):
        c = content[0]
    else:
        c = content
    repo.update_file("Casks/ver.rb", msg, cask.encode("utf-8"), c.sha)
    print("Successfully updated homebrew Cask")


if __name__ == "__main__":
    main()
